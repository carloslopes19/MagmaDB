from __future__ import annotations

import asyncio
import os
import pickle
import struct
import threading
from typing import Dict, List, Optional, IO

from voltdb.engine import VoltEngine
from voltdb.protocol import RESP, ProtocolError

_WAL_MAGIC = b"VOLTWAL\x01"
_SNAP_MAGIC = b"VOLTSNP\x01"
_DEFAULT_DIR = "data"


# ────────────────────────────────────────────────────────────────────
#  Write-Ahead Log
# ────────────────────────────────────────────────────────────────────

class Wal:
    """Append-only Write-Ahead Log with per-entry fsync.

    Every write command is durably recorded *before* the engine applies
    it.  On boot, ``recover()`` replays the WAL to restore engine state.

    File format
    -----------
    [magic:8]  entry entry ...   (each entry is a RESP array prefixed
                                   by a 4-byte big-endian length)
    """

    def __init__(self, data_dir: str = _DEFAULT_DIR) -> None:
        self._dir = data_dir
        self._path = os.path.join(data_dir, "wal.log")
        self._lock = threading.Lock()
        self._file: Optional[IO] = None
        self._open_file()

    # ── file management ──────────────────────────────────────────────

    def _open_file(self) -> None:
        os.makedirs(self._dir, exist_ok=True)
        exists = os.path.isfile(self._path)
        self._file = open(self._path, "ab")
        if not exists:
            self._file.write(_WAL_MAGIC)
            self._file.flush()
            os.fsync(self._file.fileno())

    def close(self) -> None:
        with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None

    # ── write ────────────────────────────────────────────────────────

    def append(self, args: List[bytes]) -> None:
        """Write a command to the WAL and fsync before returning."""
        entry = RESP.encode_wal_entry(args)
        with self._lock:
            if self._file is None:
                raise RuntimeError("WAL is closed")
            self._file.write(entry)
            self._file.flush()
            os.fsync(self._file.fileno())

    def append_batch(self, entries: List[List[bytes]]) -> None:
        """Write multiple commands with a single fsync."""
        if not entries:
            return
        payload = bytearray()
        for args in entries:
            payload.extend(RESP.encode_wal_entry(args))
        with self._lock:
            if self._file is None:
                raise RuntimeError("WAL is closed")
            self._file.write(payload)
            self._file.flush()
            os.fsync(self._file.fileno())

    # ── recovery ─────────────────────────────────────────────────────

    def recover(self, engine: VoltEngine) -> int:
        """Replay all WAL entries into *engine*.

        Returns the number of commands replayed.
        """
        if not os.path.isfile(self._path):
            return 0

        count = 0
        with open(self._path, "rb") as f:
            magic = f.read(len(_WAL_MAGIC))
            if magic != _WAL_MAGIC:
                return 0

            while True:
                length_bytes = f.read(4)
                if not length_bytes:
                    break
                if len(length_bytes) < 4:
                    break  # truncated entry – ignore
                length = struct.unpack(">I", length_bytes)[0]
                if length == 0:
                    break
                data = f.read(length)
                if len(data) < length:
                    break  # truncated – ignore
                try:
                    args = RESP.decode_wal_entry(data)
                except ProtocolError:
                    break  # corrupt entry – stop replay

                if not args:
                    continue

                cmd = args[0].upper()
                if cmd == b"SET" and len(args) >= 3:
                    engine.set(args[1], args[2])
                    count += 1
                elif cmd == b"DELETE" and len(args) >= 2:
                    engine.delete(args[1])
                    count += 1

        return count


# ────────────────────────────────────────────────────────────────────
#  Background Snapshotting (BGSAVE)
# ────────────────────────────────────────────────────────────────────

class Snapshotter:
    """Periodically dump the full dataset to a compact binary file
    without blocking the async event loop.

    The snapshot is a pickle of ``Dict[bytes, bytes]`` wrapped in a
    tiny header for format detection.
    """

    def __init__(self, data_dir: str = _DEFAULT_DIR) -> None:
        self._dir = data_dir
        self._path = os.path.join(data_dir, "dump.volt")
        self._lock = threading.Lock()
        self._bgsave_in_progress = False
        os.makedirs(data_dir, exist_ok=True)

    @property
    def bgsave_in_progress(self) -> bool:
        return self._bgsave_in_progress

    # ── foreground save (used by thread pool) ────────────────────────

    def _do_save(self, data: Dict[bytes, bytes]) -> None:
        """Synchronous save — intended to run in a thread pool executor."""
        tmp_path = self._path + ".tmp"
        try:
            with open(tmp_path, "wb") as f:
                f.write(_SNAP_MAGIC)
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._path)
        finally:
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass

    # ── async BGSAVE ─────────────────────────────────────────────────

    async def bgsave(self, engine: VoltEngine) -> None:
        """Trigger a non-blocking snapshot of the engine state."""
        if self._bgsave_in_progress:
            return

        self._bgsave_in_progress = True
        try:
            data = engine.snapshot()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._do_save, data)
        finally:
            self._bgsave_in_progress = False

    # ── restore ──────────────────────────────────────────────────────

    def restore_latest(self, engine: VoltEngine) -> bool:
        """Load the latest snapshot into *engine*.

        Returns ``True`` if a snapshot was found and loaded.
        """
        if not os.path.isfile(self._path):
            return False

        with open(self._path, "rb") as f:
            magic = f.read(len(_SNAP_MAGIC))
            if magic != _SNAP_MAGIC:
                return False
            try:
                data = pickle.load(f)
            except Exception:
                return False

        if isinstance(data, dict):
            engine.restore(data)
            return True
        return False
