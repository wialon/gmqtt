import asyncio

from .base import _BaseConnectionImpl
from .options import ConnectionOption


class TCPConnectionImpl(_BaseConnectionImpl):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer

    def write(self, data: bytes):
        self._writer.write(data)

    async def close(self):
        self._writer.close()

    def is_closing(self):
        return self._writer.transport.is_closing()

    async def read(self, n=-1) -> bytes:
        bs = await self._reader.read(n=n)

        if not bs or self.is_closing():
            self._writer.close()
            raise ConnectionResetError("Reset connection manually")
        return bs

    @classmethod
    async def create_connection(cls, options: ConnectionOption):
        reader, writer = await asyncio.open_connection(host=options.host,
                                                       port=options.port,
                                                       ssl=options.ssl)

        return cls(reader, writer)
