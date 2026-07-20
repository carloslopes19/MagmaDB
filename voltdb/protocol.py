from __future__ import annotations

import asyncio
import struct
from typing import Optional, List


class ProtocolError(Exception):
    """Raised on malformed RESP frames or WAL entries."""


class RESP:
    """RESP-like protocol encoder and decoder.

    Wire format (Redis Serialization Protocol simplified):
      *<n>\\r\\n  $<len>\\r\\n  <data>\\r\\n  ...
    """

    # ── Encoders ──────────────────────────────────────────────────────

    @staticmethod
    def ok() -> bytes:
        return b"+OK\r\n"

    @staticmethod
    def pong() -> bytes:
        return b"+PONG\r\n"

    @staticmethod
    def null() -> bytes:
        return b"$-1\r\n"

    @staticmethod
    def integer(val: int) -> bytes:
        return f":{val}\r\n".encode()

    @staticmethod
    def error(message: str) -> bytes:
        return f"-{message}\r\n".encode()

    @staticmethod
    def bulk_string(data: Optional[bytes]) -> bytes:
        if data is None:
            return b"$-1\r\n"
        return b"$%d\r\n%s\r\n" % (len(data), data)

    @staticmethod
    def simple_string(text: str) -> bytes:
        return f"+{text}\r\n".encode()

    @staticmethod
    def array(items: List[bytes]) -> bytes:
        buf = bytearray()
        buf.extend(b"*%d\r\n" % len(items))
        for item in items:
            buf.extend(b"$%d\r\n" % len(item))
            buf.extend(item)
            buf.extend(b"\r\n")
        return bytes(buf)

    # ── Async stream reader ───────────────────────────────────────────

    @staticmethod
    async def read_frame(
        reader: asyncio.StreamReader,
    ) -> Optional[List[bytes]]:
        """Read one RESP frame from an async stream.

        Returns the list of argument *bytes*, or None if the stream
        was cleanly closed by the peer.
        """
        line = await reader.readline()
        if not line:
            return None

        if line[0] == 42:  # ord('*')
            return await RESP._read_array(reader, line)
        else:
            return RESP._parse_inline(line)

    @staticmethod
    async def _read_array(
        reader: asyncio.StreamReader,
        header: bytes,
    ) -> List[bytes]:
        try:
            count = int(header[1:].strip())
        except (ValueError, IndexError):
            raise ProtocolError("Invalid array length")

        if count < 0:
            return []

        args: List[bytes] = []
        for _ in range(count):
            bulk_header = await reader.readline()
            if not bulk_header or bulk_header[0] != 36:  # ord('$')
                raise ProtocolError("Expected bulk string")
            try:
                length = int(bulk_header[1:].strip())
            except (ValueError, IndexError):
                raise ProtocolError("Invalid bulk string length")

            if length < 0:
                args.append(b"")
                continue

            data = await reader.readexactly(length)
            crlf = await reader.readexactly(2)
            if crlf != b"\r\n":
                raise ProtocolError("Missing CRLF after bulk string")
            args.append(data)
        return args

    @staticmethod
    def _parse_inline(line: bytes) -> Optional[List[bytes]]:
        parts = line.strip().split()
        if not parts:
            return None
        cmd = [parts[0].upper()]
        cmd.extend(parts[1:])
        return cmd

    # ── WAL binary encoding ───────────────────────────────────────────

    @staticmethod
    def encode_wal_entry(args: List[bytes]) -> bytes:
        """Encode a command as a length-prefixed WAL entry.

        Format:
          4 bytes: payload length (big-endian uint32)
          N bytes: RESP-encoded array of the command
        """
        payload = RESP.array(args)
        return struct.pack(">I", len(payload)) + payload

    @staticmethod
    def decode_wal_entry(data: bytes) -> List[bytes]:
        """Decode a WAL entry (RESP array) back to command args."""
        if not data or data[0] != 42:  # ord('*')
            raise ProtocolError("WAL entry does not start with '*'")

        crlf_pos = data.find(b"\r\n")
        if crlf_pos < 0:
            raise ProtocolError("WAL entry missing header CRLF")
        try:
            count = int(data[1:crlf_pos])
        except ValueError:
            raise ProtocolError("Invalid WAL entry array count")

        offset = crlf_pos + 2
        args: List[bytes] = []
        for _ in range(count):
            if offset >= len(data) or data[offset] != 36:  # ord('$')
                raise ProtocolError("WAL entry expected bulk string")
            crlf_pos = data.find(b"\r\n", offset)
            if crlf_pos < 0:
                raise ProtocolError("WAL entry bulk header missing CRLF")
            try:
                length = int(data[offset + 1 : crlf_pos])
            except ValueError:
                raise ProtocolError("Invalid WAL entry bulk length")
            offset = crlf_pos + 2
            if length > 0:
                args.append(data[offset : offset + length])
            else:
                args.append(b"")
            offset += length + 2  # skip data + \r\n
        return args
