from __future__ import annotations

import asyncio
import logging
from typing import List, Optional, Set

from magmadb.engine import VoltEngine
from magmadb.protocol import RESP, ProtocolError
from magmadb.storage import Wal

logger = logging.getLogger("magmadb.replication")


# ────────────────────────────────────────────────────────────────────
#  Master-side:  manages slave connections and propagates writes
# ────────────────────────────────────────────────────────────────────

class ReplicaManager:
    """Holds the set of connected replica writers and fans out every
    write command to all of them asynchronously."""

    def __init__(self) -> None:
        self._slaves: Set[asyncio.StreamWriter] = set()
        self._lock = asyncio.Lock()

    # ── management ───────────────────────────────────────────────────

    def add_slave(self, writer: asyncio.StreamWriter) -> None:
        self._slaves.add(writer)

    def remove_slave(self, writer: asyncio.StreamWriter) -> None:
        self._slaves.discard(writer)

    @property
    def slave_count(self) -> int:
        return len(self._slaves)

    # ── propagation ──────────────────────────────────────────────────

    async def propagate(self, args: List[bytes]) -> None:
        """Send a command to every connected replica.

        Failed / disconnected replicas are silently removed.
        """
        if not self._slaves:
            return

        frame = RESP.array(args)
        dead: List[asyncio.StreamWriter] = []

        async with self._lock:
            for writer in self._slaves:
                try:
                    writer.write(frame)
                    await writer.drain()
                except (OSError, ConnectionError, asyncio.IncompleteReadError):
                    dead.append(writer)

            for writer in dead:
                self._slaves.discard(writer)

    # ── full-sync for a newly-connected replica ──────────────────────

    async def full_sync(
        self,
        engine: VoltEngine,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Send the entire dataset as SET commands to a fresh replica.

        This is called *after* the initial REPLICATE handshake so the
        new replica catches up with the current state.
        """
        items = list(engine.iter_items())
        for key, value in items:
            cmd: List[bytes] = [b"SET", key, value]
            frame = RESP.array(cmd)
            try:
                writer.write(frame)
                await writer.drain()
            except (OSError, ConnectionError):
                logger.warning("Full-sync failed for a replica")
                return

        # Signal the end of the full-sync with a special marker.
        try:
            writer.write(RESP.array([b"SYNC_DONE"]))
            await writer.drain()
        except (OSError, ConnectionError):
            pass


# ────────────────────────────────────────────────────────────────────
#  Slave-side:  connects to master, receives & applies commands
# ────────────────────────────────────────────────────────────────────

class ReplicaClient:
    """Connects to a VoltDB master and continuously applies replicated
    write commands to the local engine and WAL."""

    def __init__(
        self,
        master_host: str,
        master_port: int,
        engine: VoltEngine,
        wal: Wal,
        *,
        reconnect_delay: float = 1.0,
    ) -> None:
        self._master_host = master_host
        self._master_port = master_port
        self._engine = engine
        self._wal = wal
        self._reconnect_delay = reconnect_delay
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._stop_event = asyncio.Event()

    # ── lifecycle ────────────────────────────────────────────────────

    async def run(self) -> None:
        """Connect to the master and enter the replication loop.

        This method reconnects automatically on failure until
        ``stop()`` is called.
        """
        while not self._stop_event.is_set():
            try:
                await self._connect_and_sync()
            except (OSError, ConnectionError, asyncio.IncompleteReadError) as exc:
                logger.warning("Replication connection lost: %s", exc)
            except Exception:
                logger.exception("Unexpected replication error")
            await asyncio.sleep(self._reconnect_delay)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except (OSError, ConnectionError):
                pass

    # ── connect & sync loop ──────────────────────────────────────────

    async def _connect_and_sync(self) -> None:
        logger.info(
            "Connecting to master %s:%d",
            self._master_host,
            self._master_port,
        )
        self._reader, self._writer = await asyncio.open_connection(
            self._master_host,
            self._master_port,
        )

        # Handshake: identify as a replica
        self._writer.write(RESP.array([b"REPLICATE"]))
        await self._writer.drain()

        # Wait for OK
        response = await self._reader.readline()
        if not response or response[0] != 43:  # ord('+')
            raise ConnectionError(
                f"Master did not acknowledge replication: {response!r}"
            )

        logger.info("Replication handshake complete, receiving full-sync")

        # Consume the full-sync stream
        synced = False
        while not synced and not self._stop_event.is_set():
            args = await RESP.read_frame(self._reader)
            if args is None:
                raise ConnectionError("Master closed connection")

            if len(args) == 1 and args[0] == b"SYNC_DONE":
                synced = True
                logger.info("Full-sync completed, entering streaming replication")
                break

            self._apply_command(args)

        if not synced:
            return

        # Streaming replication: read and apply commands forever
        while not self._stop_event.is_set():
            args = await RESP.read_frame(self._reader)
            if args is None:
                raise ConnectionError("Master closed connection")
            self._apply_command(args)

    # ── command application ──────────────────────────────────────────

    def _apply_command(self, args: List[bytes]) -> None:
        if not args:
            return
        cmd = args[0].upper()
        if cmd == b"SET" and len(args) >= 3:
            self._engine.set(args[1], args[2])
        elif cmd == b"DELETE" and len(args) >= 2:
            self._engine.delete(args[1])
