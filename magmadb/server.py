from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Optional, List

from magmadb.engine import VoltEngine
from magmadb.protocol import RESP, ProtocolError
from magmadb.storage import Wal, Snapshotter
from magmadb.replication import ReplicaManager, ReplicaClient

logger = logging.getLogger("magmadb.server")

_DEFAULT_PORT = 6379
_DEFAULT_MAX_KEYS = 10000
_DEFAULT_DATA_DIR = "data"
_BGSAVE_INTERVAL = 300  # seconds


# ────────────────────────────────────────────────────────────────────
#  Argument parsing
# ────────────────────────────────────────────────────────────────────

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="VoltDB – In-Memory NoSQL Database",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=_DEFAULT_PORT,
        help=f"TCP port (default {_DEFAULT_PORT})",
    )
    parser.add_argument(
        "--max-keys",
        type=int,
        default=_DEFAULT_MAX_KEYS,
        help=f"Max keys before LRU eviction (default {_DEFAULT_MAX_KEYS})",
    )
    parser.add_argument(
        "--data-dir",
        default=_DEFAULT_DATA_DIR,
        help=f"Data directory for WAL / snapshots (default '{_DEFAULT_DATA_DIR}')",
    )
    parser.add_argument(
        "--slaveof",
        default=None,
        metavar="HOST:PORT",
        help="Run as replica of the given master (e.g. 127.0.0.1:6379)",
    )
    parser.add_argument(
        "--bgsave-interval",
        type=int,
        default=_BGSAVE_INTERVAL,
        help=f"Seconds between automatic BGSAVEs (default {_BGSAVE_INTERVAL})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args(argv)


# ────────────────────────────────────────────────────────────────────
#  Command dispatch
# ────────────────────────────────────────────────────────────────────

class CommandHandler:
    """Processes individual RESP commands against the engine, WAL,
    replica manager, and snapshotter."""

    def __init__(
        self,
        engine: VoltEngine,
        wal: Wal,
        snapshotter: Snapshotter,
        replica_mgr: Optional[ReplicaManager],
        is_slave: bool,
    ) -> None:
        self._engine = engine
        self._wal = wal
        self._snapshotter = snapshotter
        self._replica_mgr = replica_mgr
        self._is_slave = is_slave

    async def dispatch(
        self,
        args: List[bytes],
        writer: asyncio.StreamWriter,
    ) -> None:
        cmd = args[0].upper() if args else b""

        try:
            if cmd == b"PING":
                writer.write(RESP.pong())

            elif cmd == b"SET":
                if self._is_slave:
                    writer.write(RESP.error("READONLY"))
                    return
                if len(args) < 3:
                    writer.write(RESP.error("ERR wrong number of arguments for 'SET'"))
                    return
                key, value = args[1], args[2]
                self._wal.append([b"SET", key, value])
                self._engine.set(key, value)
                if self._replica_mgr is not None:
                    await self._replica_mgr.propagate([b"SET", key, value])
                writer.write(RESP.ok())

            elif cmd == b"GET":
                if len(args) < 2:
                    writer.write(RESP.error("ERR wrong number of arguments for 'GET'"))
                    return
                value = self._engine.get(args[1])
                writer.write(RESP.bulk_string(value))

            elif cmd == b"DELETE":
                if self._is_slave:
                    writer.write(RESP.error("READONLY"))
                    return
                if len(args) < 2:
                    writer.write(RESP.error("ERR wrong number of arguments for 'DELETE'"))
                    return
                self._wal.append([b"DELETE", args[1]])
                deleted = self._engine.delete(args[1])
                if self._replica_mgr is not None:
                    await self._replica_mgr.propagate([b"DELETE", args[1]])
                writer.write(RESP.integer(1 if deleted else 0))

            elif cmd == b"EXISTS":
                if len(args) < 2:
                    writer.write(RESP.error("ERR wrong number of arguments for 'EXISTS'"))
                    return
                writer.write(RESP.integer(1 if self._engine.has(args[1]) else 0))

            elif cmd == b"DBSIZE":
                writer.write(RESP.integer(self._engine.size))

            elif cmd == b"FLUSHALL":
                if self._is_slave:
                    writer.write(RESP.error("READONLY"))
                    return
                self._engine.clear()
                writer.write(RESP.ok())

            elif cmd == b"BGSAVE":
                if self._is_slave:
                    writer.write(RESP.error("READONLY"))
                    return
                await self._snapshotter.bgsave(self._engine)
                writer.write(RESP.simple_string("Background saving started"))

            elif cmd == b"REPLICATE":
                if self._replica_mgr is None:
                    writer.write(RESP.error("ERR this instance is a slave"))
                    return
                writer.write(RESP.ok())
                await writer.drain()
                await self._replica_mgr.full_sync(self._engine, writer)
                self._replica_mgr.add_slave(writer)
                logger.info("New replica connected and synced")

            elif cmd == b"SLAVEOF":
                if len(args) < 2:
                    writer.write(RESP.error("ERR wrong number of arguments for 'SLAVEOF'"))
                    return
                writer.write(RESP.error("ERR SLAVEOF not supported dynamically yet"))

            elif cmd == b"INFO":
                role = "slave" if self._is_slave else "master"
                connected = (
                    0
                    if self._replica_mgr is None
                    else self._replica_mgr.slave_count
                )
                info = (
                    f"# Server\r\n"
                    f"role:{role}\r\n"
                    f"connected_slaves:{connected}\r\n"
                    f"db_size:{self._engine.size}\r\n"
                    f"max_keys:{self._engine.max_keys}\r\n"
                )
                writer.write(RESP.bulk_string(info.encode()))

            else:
                writer.write(
                    RESP.error(f"ERR unknown command '{cmd.decode(errors='replace')}'")
                )

        except ProtocolError:
            writer.write(RESP.error("ERR protocol error"))
        except (OSError, ConnectionError):
            raise

        try:
            await writer.drain()
        except (OSError, ConnectionError):
            pass


# ────────────────────────────────────────────────────────────────────
#  Client connection handler
# ────────────────────────────────────────────────────────────────────

async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    handler: CommandHandler,
    replica_mgr: Optional[ReplicaManager] = None,
) -> None:
    addr = writer.get_extra_info("peername")
    logger.debug("New connection from %s", addr)

    try:
        while True:
            try:
                args = await RESP.read_frame(reader)
            except ProtocolError as exc:
                writer.write(RESP.error(f"ERR {exc}"))
                await writer.drain()
                continue
            except (OSError, ConnectionError):
                break

            if args is None:
                break

            await handler.dispatch(args, writer)

    except (OSError, ConnectionError):
        pass
    except asyncio.IncompleteReadError:
        pass
    except Exception:
        logger.exception("Unhandled error handling client %s", addr)
    finally:
        logger.debug("Connection closed from %s", addr)
        if replica_mgr is not None:
            replica_mgr.remove_slave(writer)
        try:
            writer.close()
            await writer.wait_closed()
        except (OSError, ConnectionError):
            pass


# ────────────────────────────────────────────────────────────────────
#  Periodic BGSAVE task
# ────────────────────────────────────────────────────────────────────

async def _periodic_bgsave(
    snapshotter: Snapshotter,
    engine: VoltEngine,
    interval: int,
) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await snapshotter.bgsave(engine)
        except Exception:
            logger.exception("Periodic BGSAVE failed")


# ────────────────────────────────────────────────────────────────────
#  Main entry point
# ────────────────────────────────────────────────────────────────────

async def _async_main(args: argparse.Namespace) -> None:
    # ── Bootstrap ────────────────────────────────────────────────
    is_slave = args.slaveof is not None

    engine = VoltEngine(max_keys=args.max_keys)
    wal = Wal(data_dir=args.data_dir)
    snapshotter = Snapshotter(data_dir=args.data_dir)

    logger.info("Recovering state...")
    snapshot_loaded = snapshotter.restore_latest(engine)
    wal_count = wal.recover(engine)
    logger.info(
        "Recovery complete: snapshot=%s, wal_replayed=%d",
        snapshot_loaded,
        wal_count,
    )

    # ── Replication setup ────────────────────────────────────────
    replica_mgr: Optional[ReplicaManager] = None
    replica_client: Optional[ReplicaClient] = None

    if is_slave:
        host, port_str = args.slaveof.split(":")
        port = int(port_str)
        replica_client = ReplicaClient(host, port, engine, wal)
        asyncio.create_task(replica_client.run())
        logger.info("Running as slave of %s:%d", host, port)
    else:
        replica_mgr = ReplicaManager()
        logger.info("Running as master")

    handler = CommandHandler(engine, wal, snapshotter, replica_mgr, is_slave)

    # ── Periodic BGSAVE (master only) ────────────────────────────
    if not is_slave and args.bgsave_interval > 0:
        asyncio.create_task(
            _periodic_bgsave(snapshotter, engine, args.bgsave_interval)
        )

    # ── Start TCP server ─────────────────────────────────────────
    server = await asyncio.start_server(
        lambda r, w: _handle_client(r, w, handler, replica_mgr),
        host="0.0.0.0",
        port=args.port,
    )

    addr = server.sockets[0].getsockname()
    logger.info("VoltDB listening on %s:%d", addr[0], addr[1])
    print(f"VoltDB started on port {args.port} (role={'slave' if is_slave else 'master'})")

    # ── Graceful shutdown ────────────────────────────────────────
    try:
        async with server:
            await server.serve_forever()
    except asyncio.CancelledError:
        pass

    # ── Cleanup ──────────────────────────────────────────────────
    logger.info("Shutting down...")
    if replica_client is not None:
        await replica_client.stop()

    if not is_slave:
        logger.info("Performing final BGSAVE before shutdown")
        await snapshotter.bgsave(engine)

    wal.close()
    logger.info("VoltDB shutdown complete")


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
