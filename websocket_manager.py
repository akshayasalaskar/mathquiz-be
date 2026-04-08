"""Track WebSocket connections and broadcast to all clients."""

import asyncio
from typing import Optional

from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        # username -> WebSocket (at most one connection per username)
        self._active: dict[str, WebSocket] = {}
        self._lock = asyncio.Lock()

    async def try_accept(
        self, username: str, websocket: WebSocket
    ) -> tuple[bool, Optional[str]]:
        """
        Accept connection if username is free. Returns (ok, error_message).
        """
        await websocket.accept()
        async with self._lock:
            if username in self._active:
                return False, "Username already taken"
            self._active[username] = websocket
        return True, None

    def disconnect(self, username: str) -> None:
        self._active.pop(username, None)

    async def send_personal(self, username: str, message: dict) -> None:
        ws = self._active.get(username)
        if ws is not None:
            try:
                await ws.send_json(message)
            except Exception:
                pass

    async def broadcast_json(self, message: dict) -> None:
        dead: list[str] = []
        for uname, ws in list(self._active.items()):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(uname)
        for uname in dead:
            self._active.pop(uname, None)


_manager: Optional[ConnectionManager] = None


def get_manager() -> ConnectionManager:
    global _manager
    if _manager is None:
        _manager = ConnectionManager()
    return _manager
