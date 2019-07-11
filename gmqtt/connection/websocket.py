import aiohttp
import asyncio


from .base import _BaseConnectionImpl
from .options import ConnectionOption


class WebSocketConnectionImpl(_BaseConnectionImpl):
    def __init__(self, session: aiohttp.ClientSession,  ws_response: aiohttp.ClientWebSocketResponse):
        self._session = session
        self._ws_response = ws_response
        self._lock = asyncio.Lock()

    def write(self, data: bytes):
        asyncio.ensure_future(self._ws_response.send_bytes(data))

    def is_closing(self):
        return self._ws_response.closed

    async def close(self):
        await self._ws_response.close()
        await self._session.close()

    async def read(self, n=-1) -> bytes:
        # need lock, because aiohttp-ws don't allow concurrent reads
        async with self._lock:
            bs = await self._ws_response.receive_bytes()

        if not bs or self._ws_response.closed:
            raise ConnectionResetError("Reset connection manually")
        return bs

    @classmethod
    async def create_connection(cls, options: ConnectionOption):
        session = aiohttp.ClientSession()

        ws_response = await session.ws_connect(options.url)

        return cls(session, ws_response)


