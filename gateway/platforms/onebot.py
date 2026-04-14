"""
OneBot v11 platform adapter for Hermes Agent.

Connects to a NapCat (or any OneBot v11 compatible) QQ bot server
via WebSocket (reverse WebSocket / forward WebSocket).

Reference: https://github.com/botuniverse/onebot-11
Inspired by: https://github.com/CharTyr/napcat-plugin-openclaw
"""

import asyncio
import json
import logging
import os
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import websockets
from websockets.asyncio.client import connect as ws_connect

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageType,
    MessageEvent,
    SendResult,
    SessionSource,
    cache_audio_from_url,
    cache_image_from_url,
    cache_document_from_bytes,
)

logger = logging.getLogger(__name__)

# OneBot v11 WebSocket subtypes
_WS_SUBTYPE_NONE = 0


def check_onebot_requirements() -> bool:
    """Check if dependencies for the OneBot adapter are available."""
    try:
        import websockets  # noqa: F401
        return True
    except ImportError:
        return False


class OneBotAdapter(BasePlatformAdapter):
    """
    OneBot v11 adapter that connects to a QQ bot via WebSocket.

    Supports:
    - Forward WebSocket (connects to ws://host:port)
    - Reverse WebSocket (listens on a port — not implemented yet)
    - Private and group messages
    - Text, image, voice, video, file, reply, @mention
    """

    MAX_MESSAGE_LENGTH = 4500  # QQ message limit
    SUPPORTS_MESSAGE_EDITING = False  # OneBot/QQ doesn't support editing sent messages
    PREFERRED_SPLIT = "\n\n"  # Prefer splitting at paragraph boundaries

    def __init__(self, config):
        super().__init__(config, platform=None)
        # Import Platform here to avoid circular import
        from gateway.config import Platform
        self.platform = Platform.ONEBOT

        # Connection settings
        self._ws_url = config.extra.get(
            "ws_url", os.getenv("ONEBOT_WS_URL", "ws://127.0.0.1:3001")
        )
        self._access_token = config.extra.get(
            "access_token", os.getenv("ONEBOT_ACCESS_TOKEN", "")
        )
        self._bot_id: Optional[int] = None
        bot_id_str = config.extra.get(
            "bot_id", os.getenv("ONEBOT_BOT_ID", "")
        )
        if bot_id_str:
            try:
                self._bot_id = int(bot_id_str)
            except ValueError:
                pass
        self._reconnect_delay = config.extra.get("reconnect_delay", 5.0)

        # Behavior settings
        self._private_chat_enabled = config.extra.get(
            "private_chat", os.getenv("ONEBOT_PRIVATE_CHAT", "true").lower() in ("true", "1", "yes")
        )
        self._group_at_only = config.extra.get(
            "group_at_only", os.getenv("ONEBOT_GROUP_AT_ONLY", "true").lower() in ("true", "1", "yes")
        )
        self._group_session_mode = config.extra.get(
            "group_session_mode", os.getenv("ONEBOT_GROUP_SESSION_MODE", "user")
        )  # "user" or "shared"

        # Debounce settings (merge rapid messages from same user)
        self._debounce_ms = config.extra.get(
            "debounce_ms", int(os.getenv("ONEBOT_DEBOUNCE_MS", "2000"))
        )
        self._debounce_timers: Dict[str, asyncio.TimerHandle] = {}
        self._debounce_buffers: Dict[str, dict] = {}

        # State
        self._ws = None
        self._bot_nickname: str = ""
        self._listen_task: Optional[asyncio.Task] = None
        self._should_reconnect = True

        # WebSocket API call infrastructure (request/response over WS)
        self._ws_api_lock = asyncio.Lock()
        self._ws_api_futures: Dict[str, asyncio.Future] = {}
        self._ws_api_counter = 0

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Connect to the OneBot WebSocket and start listening."""
        # Bypass system proxy for the WebSocket connection to the QQ bot server.
        # macOS sets a system HTTP proxy that websockets will pick up automatically,
        # breaking the direct WebSocket connection to the NapCat server.
        try:
            from urllib.parse import urlparse
            hostname = urlparse(self._ws_url).hostname
            if hostname:
                no_proxy = os.environ.get("NO_PROXY", "")
                hosts = [h.strip() for h in no_proxy.split(",") if h.strip()]
                if hostname not in hosts and f".{hostname}" not in hosts:
                    hosts.append(hostname)
                    os.environ["NO_PROXY"] = ",".join(hosts)
        except Exception:
            os.environ["NO_PROXY"] = os.environ.get("NO_PROXY", "") + ",*"
        try:
            headers = {}
            if self._access_token:
                headers["Authorization"] = f"Bearer {self._access_token}"

            logger.info("[OneBot] Connecting to %s ...", self._redact_url(self._ws_url))
            self._ws = await ws_connect(
                self._ws_url,
                additional_headers=headers,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
            )
            logger.info("[OneBot] WebSocket connected")

            # Fetch bot info if we don't know our ID yet
            if not self._bot_id:
                try:
                    bot_info = await self._api_call("get_login_info")
                    if bot_info:
                        self._bot_id = int(bot_info.get("user_id", 0))
                        self._bot_nickname = bot_info.get("nickname", "")
                        logger.info("[OneBot] Logged in as %s (QQ: %s)", self._bot_nickname, self._bot_id)
                except Exception as e:
                    logger.warning("[OneBot] Failed to get login info: %s", e)

            self._should_reconnect = True
            self._listen_task = asyncio.create_task(self._listen_loop())
            self._mark_connected()
            return True

        except Exception as e:
            logger.error("[OneBot] Connection failed: %s", e)
            self._set_fatal_error("connect_failed", str(e), retryable=True)
            return False

    async def disconnect(self) -> None:
        """Disconnect from the OneBot WebSocket."""
        self._should_reconnect = False
        for key, timer in self._debounce_timers.items():
            timer.cancel()
        self._debounce_timers.clear()
        self._debounce_buffers.clear()

        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        self._mark_disconnected()
        logger.info("[OneBot] Disconnected")

    # ------------------------------------------------------------------
    # WebSocket Listen Loop
    # ------------------------------------------------------------------

    async def _listen_loop(self) -> None:
        """Main loop that receives events from the WebSocket."""
        backoff = self._reconnect_delay
        while self._should_reconnect:
            try:
                async for raw in self._ws:
                    try:
                        event = json.loads(raw)
                        await self._handle_event(event)
                    except json.JSONDecodeError:
                        logger.warning("[OneBot] Invalid JSON received")
                    except Exception as e:
                        logger.error("[OneBot] Error handling event: %s", e)
                # WebSocket closed normally
                logger.info("[OneBot] WebSocket connection closed")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("[OneBot] WebSocket error: %s", e)

            if not self._should_reconnect:
                return

            # Reconnect with exponential backoff
            logger.info("[OneBot] Reconnecting in %.1fs ...", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

            try:
                headers = {}
                if self._access_token:
                    headers["Authorization"] = f"Bearer {self._access_token}"
                self._ws = await ws_connect(
                    self._ws_url,
                    additional_headers=headers,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                )
                logger.info("[OneBot] Reconnected")
                backoff = self._reconnect_delay  # Reset backoff
            except Exception as e:
                logger.error("[OneBot] Reconnect failed: %s", e)

    # ------------------------------------------------------------------
    # Event Handling
    # ------------------------------------------------------------------

    async def _handle_event(self, event: dict) -> None:
        """Dispatch incoming OneBot events."""
        # Check if this is an API response (has "echo" field = our request ID)
        echo = event.get("echo")
        if echo is not None:
            future = self._ws_api_futures.pop(echo, None)
            if future and not future.done():
                future.set_result(event)
            return

        post_type = event.get("post_type", "")

        if post_type == "message":
            await self._handle_message_event(event)
        elif post_type == "meta_event":
            # Heartbeat, lifecycle — ignore
            pass
        elif post_type == "notice":
            # Group member join/leave, etc. — ignore for now
            pass
        elif post_type == "request":
            # Friend/group requests — ignore for now
            pass

    async def _handle_message_event(self, event: dict) -> None:
        """Handle a OneBot message event (private or group)."""
        message_type = event.get("message_type", "")  # "private" or "group"
        sub_type = event.get("sub_type", "")
        user_id = event.get("user_id")
        group_id = event.get("group_id")
        message_id = event.get("message_id")
        raw_message = event.get("message", [])
        sender = event.get("sender", {})
        nickname = sender.get("nickname", "") or sender.get("card", "") or str(user_id)

        # Skip self-messages
        if user_id == self._bot_id:
            return

        # Filter private/group based on config
        if message_type == "private" and not self._private_chat_enabled:
            return

        if message_type == "group":
            if self._group_at_only:
                # Check if bot is @mentioned
                if not self._is_bot_mentioned(raw_message):
                    return

        # Build session key
        session_key = self._build_session_key(message_type, user_id, group_id)

        # Extract message content
        text, media_urls, media_types = await self._extract_message(raw_message, event)

        # Handle reply context
        reply_to_id = None
        reply_to_text = None
        for seg in (raw_message if isinstance(raw_message, list) else []):
            if seg.get("type") == "reply":
                reply_to_id = seg.get("data", {}).get("id")
                if reply_to_id:
                    reply_to_text = await self._get_reply_text(reply_to_id)

        # Build source
        source = self.build_source(
            chat_id=session_key,
            user_id=str(user_id),
            user_name=nickname,
            chat_type="group" if message_type == "group" else "dm",
            chat_name=str(group_id) if message_type == "group" else None,
        )

        # Determine message type
        msg_type = MessageType.TEXT
        if media_urls:
            first_media_type = media_types[0] if media_types else "image"
            if first_media_type == "image":
                msg_type = MessageType.PHOTO
            elif first_media_type in ("voice", "record"):
                msg_type = MessageType.VOICE
            elif first_media_type == "video":
                msg_type = MessageType.VIDEO
            elif first_media_type == "file":
                msg_type = MessageType.DOCUMENT

        # Debounce: merge rapid messages from the same session
        if self._debounce_ms > 0 and text:
            self._schedule_debounced_message(
                session_key=session_key,
                text=text,
                media_urls=media_urls,
                media_types=media_types,
                msg_type=msg_type,
                source=source,
                message_id=str(message_id) if message_id else None,
                reply_to_id=reply_to_id,
                reply_to_text=reply_to_text,
            )
        else:
            msg_event = MessageEvent(
                text=text,
                message_type=msg_type,
                source=source,
                raw_message=event,
                message_id=str(message_id) if message_id else None,
                media_urls=media_urls,
                media_types=media_types,
                reply_to_message_id=str(reply_to_id) if reply_to_id else None,
                reply_to_text=reply_to_text,
                timestamp=datetime.now(),
            )
            await self.handle_message(msg_event)

    # ------------------------------------------------------------------
    # Debounce
    # ------------------------------------------------------------------

    def _schedule_debounced_message(
        self,
        session_key: str,
        text: str,
        media_urls: list,
        media_types: list,
        msg_type: MessageType,
        source: SessionSource,
        message_id: Optional[str],
        reply_to_id: Optional[str],
        reply_to_text: Optional[str],
    ) -> None:
        """Schedule a debounced message. Merges rapid messages from the same session."""
        if session_key in self._debounce_buffers:
            buf = self._debounce_buffers[session_key]
            if text:
                buf["text"] += "\n" + text
            buf["media_urls"].extend(media_urls)
            buf["media_types"].extend(media_types)
            buf["message_id"] = message_id
            # Cancel previous timer
            self._debounce_timers[session_key].cancel()
        else:
            self._debounce_buffers[session_key] = {
                "text": text,
                "media_urls": list(media_urls),
                "media_types": list(media_types),
                "msg_type": msg_type,
                "source": source,
                "message_id": message_id,
                "reply_to_id": reply_to_id,
                "reply_to_text": reply_to_text,
            }

        loop = asyncio.get_event_loop()
        self._debounce_timers[session_key] = loop.call_later(
            self._debounce_ms / 1000.0,
            lambda: asyncio.ensure_future(self._flush_debounced(session_key)),
        )

    async def _flush_debounced(self, session_key: str) -> None:
        """Flush the debounced buffer and dispatch the message."""
        buf = self._debounce_buffers.pop(session_key, None)
        self._debounce_timers.pop(session_key, None)
        if not buf:
            return

        msg_event = MessageEvent(
            text=buf["text"],
            message_type=buf["msg_type"],
            source=buf["source"],
            raw_message=None,
            message_id=buf["message_id"],
            media_urls=buf["media_urls"],
            media_types=buf["media_types"],
            reply_to_message_id=str(buf["reply_to_id"]) if buf["reply_to_id"] else None,
            reply_to_text=buf["reply_to_text"],
            timestamp=datetime.now(),
        )
        await self.handle_message(msg_event)

    # ------------------------------------------------------------------
    # Message Extraction
    # ------------------------------------------------------------------

    async def _extract_message(
        self, message: Any, event: dict
    ) -> Tuple[str, List[str], List[str]]:
        """
        Extract text and media from OneBot v11 message segments.

        Returns: (text, media_urls, media_types)
        """
        if isinstance(message, str):
            return message.strip(), [], []

        if not isinstance(message, list):
            return str(message).strip(), [], []

        text_parts: List[str] = []
        media_urls: List[str] = []
        media_types: List[str] = []

        for seg in message:
            seg_type = seg.get("type", "")
            data = seg.get("data", {})

            if seg_type == "text":
                t = data.get("text", "").strip()
                if t:
                    text_parts.append(t)

            elif seg_type == "image":
                url = data.get("url", "")
                if url:
                    try:
                        cached = await cache_image_from_url(url)
                        media_urls.append(cached)
                        media_types.append("image")
                    except Exception as e:
                        logger.warning("[OneBot] Failed to cache image: %s", e)

            elif seg_type == "record":
                url = data.get("url", "")
                if url:
                    try:
                        cached = await cache_audio_from_url(url, ext=".silk")
                        media_urls.append(cached)
                        media_types.append("voice")
                    except Exception:
                        # Try as ogg
                        try:
                            cached = await cache_audio_from_url(url, ext=".ogg")
                            media_urls.append(cached)
                            media_types.append("voice")
                        except Exception as e:
                            logger.warning("[OneBot] Failed to cache voice: %s", e)

            elif seg_type == "video":
                url = data.get("url", "")
                if url:
                    try:
                        cached = await cache_image_from_url(url, ext=".mp4")
                        media_urls.append(cached)
                        media_types.append("video")
                    except Exception as e:
                        logger.warning("[OneBot] Failed to cache video: %s", e)

            elif seg_type == "file":
                url = data.get("url", "")
                if url:
                    # Download file via WebSocket API (get_file) or skip
                    logger.debug("[OneBot] File segment received (url=%s), skipping download", url)

            elif seg_type == "at":
                qq = data.get("qq", "")
                name = data.get("name", "") or qq
                # Skip @mentioning self
                if str(qq) != str(self._bot_id):
                    text_parts.append(f"@{name}")

            elif seg_type == "face":
                # QQ emoji — ignore or convert to text
                face_id = data.get("id", "")
                text_parts.append(f"[表情:{face_id}]")

            elif seg_type == "reply":
                # Reply context is handled separately
                pass

            else:
                # Unknown segment type — try to extract text
                if data.get("text"):
                    text_parts.append(data["text"])

        return " ".join(text_parts), media_urls, media_types

    # ------------------------------------------------------------------
    # Message Cleanup
    # ------------------------------------------------------------------

    @staticmethod
    def clean_message(text: str) -> str:
        """Clean message text for QQ: strip whitespace, collapse empty lines."""
        # Collapse 3+ consecutive newlines to 2 (preserving paragraph breaks)
        text = re.sub(r"\n{3,}", "\n\n", text)
        # Strip leading/trailing whitespace and empty lines
        text = text.strip()
        # Remove leading empty lines after strip
        text = re.sub(r"^\n+", "", text)
        # Remove trailing empty lines before end
        text = re.sub(r"\n+$", "", text)
        return text

    # ------------------------------------------------------------------
    # Sending Messages
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message via OneBot WebSocket API.

        Paragraph-level splitting is handled by the stream consumer
        (paragraph mode), so this method just cleans and sends the text
        as-is.
        """
        try:
            content = self.clean_message(content)
            if not content:
                return SendResult(success=True, message_id=None)

            params = self._parse_chat_id(chat_id)
            params["message"] = content
            if reply_to:
                params["message_id"] = int(reply_to)

            result = await self._api_call(
                "send_group_msg" if params.get("group_id") else "send_msg",
                params,
            )

            # Fallback: use send_msg with message_type
            if not result or result.get("retcode", -1) != 0:
                result = await self._api_call("send_msg", {
                    "message_type": params.get("message_type", "private"),
                    "user_id": params.get("user_id"),
                    "group_id": params.get("group_id"),
                    "message": content,
                })

            msg_id = result.get("data", {}).get("message_id") if result else None
            return SendResult(
                success=True,
                message_id=str(msg_id) if msg_id else None,
                raw_response=result,
            )

        except Exception as e:
            err_str = str(e).lower()
            retryable = any(
                pat in err_str
                for pat in ("connection", "timeout", "refused", "broken pipe", "eof")
            )
            return SendResult(
                success=False,
                error=str(e),
                retryable=retryable,
            )

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image via OneBot HTTP API using CQ code."""
        try:
            params = self._parse_chat_id(chat_id)
            segments = [f"[CQ:image,file={image_url}]"]
            if caption:
                segments.insert(0, caption)
            message = "\n".join(segments)
            params["message"] = message

            await self._api_call(
                "send_msg",
                params,
            )
            return SendResult(success=True)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send a voice message via OneBot HTTP API."""
        try:
            params = self._parse_chat_id(chat_id)
            params["message"] = f"[CQ:record,file=file://{audio_path}]"
            await self._api_call("send_msg", params)
            return SendResult(success=True)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send a file via OneBot HTTP API."""
        try:
            params = self._parse_chat_id(chat_id)
            segments = [f"[CQ:file,file=file://{file_path}]"]
            if caption:
                segments.insert(0, caption)
            params["message"] = "\n".join(segments)
            await self._api_call("send_msg", params)
            return SendResult(success=True)
        except Exception as e:
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """OneBot v11 doesn't have a standard typing indicator API. No-op."""
        pass

    async def get_chat_info(self, chat_id: str) -> dict:
        """Get chat/group info."""
        params = self._parse_chat_id(chat_id)
        if params.get("group_id"):
            result = await self._api_call("get_group_info", {"group_id": params["group_id"]})
            if result and result.get("retcode") == 0:
                data = result.get("data", {})
                return {
                    "name": data.get("group_name", str(params["group_id"])),
                    "type": "group",
                    "chat_id": chat_id,
                }
        return {
            "name": chat_id,
            "type": "private",
            "chat_id": chat_id,
        }

    # ------------------------------------------------------------------
    # OneBot WebSocket API
    # ------------------------------------------------------------------

    async def _api_call(self, action: str, params: Optional[dict] = None, timeout: float = 15.0) -> Optional[dict]:
        """Make an OneBot v11 API call over WebSocket (request/response with echo)."""
        if not self._ws:
            return None
        try:
            from websockets.protocol import State
            if self._ws.state != State.OPEN:
                logger.error("[OneBot] Cannot call API '%s': WebSocket not connected (state=%s)", action, self._ws.state)
                return None
        except Exception:
            pass

        self._ws_api_counter += 1
        echo = f"hermes_{self._ws_api_counter}"
        payload = {
            "action": action,
            "params": params or {},
            "echo": echo,
        }

        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._ws_api_futures[echo] = future

        try:
            async with self._ws_api_lock:
                await self._ws.send(json.dumps(payload))
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            logger.warning("[OneBot] API call '%s' timed out after %.1fs", action, timeout)
            return None
        except Exception as e:
            logger.error("[OneBot] API call '%s' failed: %s", action, e)
            return None
        finally:
            self._ws_api_futures.pop(echo, None)

    async def _get_reply_text(self, message_id: str) -> Optional[str]:
        """Get the text content of a replied-to message."""
        try:
            result = await self._api_call("get_msg", {"message_id": int(message_id)})
            if result and result.get("retcode") == 0:
                msg_data = result.get("data", {})
                raw_msg = msg_data.get("message", [])
                if isinstance(raw_msg, str):
                    return raw_msg
                if isinstance(raw_msg, list):
                    texts = []
                    for seg in raw_msg:
                        if seg.get("type") == "text":
                            t = seg.get("data", {}).get("text", "")
                            if t:
                                texts.append(t)
                    return " ".join(texts) if texts else None
        except Exception as e:
            logger.debug("[OneBot] Failed to get reply text: %s", e)
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _parse_chat_id(self, chat_id: str) -> dict:
        """
        Parse a Hermes session key back into OneBot API parameters.

        Session keys are formatted as:
        - Private: "qq-{user_id}"
        - Group (user mode): "qq-g{group_id}-{user_id}"
        - Group (shared mode): "qq-g{group_id}"
        """
        params = {}

        # Match group session patterns
        m = re.match(r"^qq-g(\d+)-(\d+)$", chat_id)
        if m:
            params["group_id"] = int(m.group(1))
            params["user_id"] = int(m.group(2))
            params["message_type"] = "group"
            return params

        m = re.match(r"^qq-g(\d+)$", chat_id)
        if m:
            params["group_id"] = int(m.group(1))
            params["message_type"] = "group"
            return params

        # Private chat
        m = re.match(r"^qq-(\d+)$", chat_id)
        if m:
            params["user_id"] = int(m.group(1))
            params["message_type"] = "private"
            return params

        # Fallback: treat as user_id
        params["user_id"] = chat_id
        params["message_type"] = "private"
        return params

    def _build_session_key(self, message_type: str, user_id, group_id) -> str:
        """Build a session key from message metadata."""
        if message_type == "private":
            return f"qq-{user_id}"
        if self._group_session_mode == "shared" and group_id:
            return f"qq-g{group_id}"
        if group_id:
            return f"qq-g{group_id}-{user_id}"
        return f"qq-{user_id}"

    def _is_bot_mentioned(self, message: Any) -> bool:
        """Check if the bot is @mentioned in the message."""
        if not self._bot_id:
            return True  # If we don't know our ID, respond to everything

        if isinstance(message, list):
            for seg in message:
                if seg.get("type") == "at":
                    qq = seg.get("data", {}).get("qq", "")
                    if str(qq) == str(self._bot_id):
                        return True
        return False

    @staticmethod
    def _redact_url(url: str) -> str:
        """Redact token/credentials from URL for logging."""
        # Simple redaction: hide query params
        if "?" in url:
            return url.split("?")[0] + "?***"
        return url
