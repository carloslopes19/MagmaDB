"""Testes rápidos: engine, WAL, snapshot, server."""

import asyncio, os, shutil, socket, subprocess, sys, time

CRLF = b"\r\n"
DIR = "_test_magmadb"
PORT = 16379


# ── 1. Unit: engine ────────────────────────────────────────────────
def test_engine():
    from magmadb.engine import VoltEngine
    e = VoltEngine(max_keys=3)
    e.set(b"a", b"1"); e.set(b"b", b"2"); e.set(b"c", b"3")
    e.get(b"a")  # move 'a' to head
    e.set(b"d", b"4")  # evicts 'b'
    assert e.get(b"a") == b"1"; assert e.get(b"b") is None
    assert e.size == 3
    snap = e.snapshot()
    e2 = VoltEngine(max_keys=10); e2.restore(snap)
    assert e2.get(b"d") == b"4"
    print("[OK] engine")


# ── 2. Unit: WAL ───────────────────────────────────────────────────
def test_wal():
    from magmadb.engine import VoltEngine
    from magmadb.storage import Wal
    d = "_test_wal"
    if os.path.isdir(d): shutil.rmtree(d)
    w = Wal(data_dir=d)
    w.append([b"SET", b"x", b"10"]); w.append([b"SET", b"y", b"20"])
    w.close()
    e = VoltEngine(max_keys=100)
    w2 = Wal(data_dir=d); n = w2.recover(e); w2.close()
    assert n == 2; assert e.get(b"x") == b"10"; assert e.get(b"y") == b"20"
    shutil.rmtree(d)
    print("[OK] wal")


# ── 3. Unit: snapshot ──────────────────────────────────────────────
async def test_snapshot():
    from magmadb.engine import VoltEngine
    from magmadb.storage import Snapshotter
    d = "_test_snap"
    if os.path.isdir(d): shutil.rmtree(d)
    e = VoltEngine(max_keys=100)
    e.set(b"k", b"v"); e.set(b"k2", b"v2")
    s = Snapshotter(data_dir=d); await s.bgsave(e)
    e2 = VoltEngine(max_keys=100)
    s2 = Snapshotter(data_dir=d); ok = s2.restore_latest(e2)
    assert ok; assert e2.get(b"k") == b"v"; assert e2.get(b"k2") == b"v2"
    shutil.rmtree(d)
    print("[OK] snapshot")


# ── 4. Integration: server ─────────────────────────────────────────
def test_server():
    if os.path.isdir(DIR): shutil.rmtree(DIR, ignore_errors=True)
    def sc(cmd):
        s = socket.socket(); s.settimeout(4)
        s.connect(("127.0.0.1", PORT)); s.sendall(cmd)
        r = s.recv(4096); s.close(); return r

    p = subprocess.Popen([sys.executable, "-m", "magmadb.server",
                          "--port", str(PORT), "--max-keys", "100",
                          "--data-dir", DIR, "--bgsave-interval", "0"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1.5)
    try:
        assert sc(b"*1" + CRLF + b"$4" + CRLF + b"PING" + CRLF) == b"+PONG\r\n"
        sc(b"*3" + CRLF + b"$3" + CRLF + b"SET" + CRLF +
           b"$1" + CRLF + b"a" + CRLF + b"$1" + CRLF + b"1" + CRLF)
        assert sc(b"*2" + CRLF + b"$3" + CRLF + b"GET" + CRLF +
                  b"$1" + CRLF + b"a" + CRLF) == b"$1\r\n1\r\n"
        assert sc(b"*2" + CRLF + b"$6" + CRLF + b"DELETE" + CRLF +
                  b"$1" + CRLF + b"a" + CRLF) == b":1\r\n"
        print("[OK] server")
    finally:
        p.terminate(); p.wait(timeout=3)
        time.sleep(0.3)
        shutil.rmtree(DIR, ignore_errors=True)


# ── Run all ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_engine()
    test_wal()
    asyncio.run(test_snapshot())
    test_server()
    print("\nTodos os testes passaram!")
