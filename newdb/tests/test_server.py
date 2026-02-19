import base64
import json
import os
import socket
import subprocess
import time


def pickFreePort():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def writeConfig(path, port, dataDir):
    with open(path, "w", encoding="utf-8") as f:
        f.write("host: 127.0.0.1\n")
        f.write(f"port: {port}\n")
        f.write(f"dataDir: {dataDir}\n")
        f.write("maxLineBytes: 1048576\n")
        f.write("maxConnections: 128\n")
        f.write("walFsync: periodic\n")
        f.write("walFsyncIntervalMs: 50\n")
        f.write("walFsyncBytes: 1048576\n")
        f.write("memtableMaxBytes: 33554432\n")
        f.write("sstableIndexStride: 16\n")


def waitForListening(proc, timeoutSec=3.0):
    start = time.time()
    while time.time() - start < timeoutSec:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                raise RuntimeError("server exited")
            continue
        if "Listening host=" in line:
            return
    raise RuntimeError("server not listening")


def tcpQuery(host, port, sql):
    s = socket.create_connection((host, port), timeout=2)
    s.sendall((sql + "\n").encode("utf-8"))
    data = b""
    while not data.endswith(b"\n"):
        chunk = s.recv(4096)
        if not chunk:
            raise RuntimeError("server closed")
        data += chunk
    s.close()
    return json.loads(data.decode("utf-8").strip())


def tcpSession(host, port, sqlList):
    s = socket.create_connection((host, port), timeout=2)
    buf = b""

    def recvLine():
        nonlocal buf
        while b"\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                raise RuntimeError("server closed")
            buf += chunk
        line, rest = buf.split(b"\n", 1)
        buf = rest
        return line.decode("utf-8").strip()

    out = []
    for sql in sqlList:
        s.sendall((sql + "\n").encode("utf-8"))
        out.append(json.loads(recvLine()))
    s.close()
    return out


def mustOk(r):
    assert r["ok"] is True
    return r


def ensureSchema(host, port):
    mustOk(tcpQuery(host, port, "CREATE KEYSPACE IF NOT EXISTS myapp;"))
    mustOk(
        tcpQuery(
            host,
            port,
            "CREATE TABLE IF NOT EXISTS myapp.users (id int64, name varchar, active boolean, born date, createdAt timestamp, avatar binary, PRIMARY KEY (id));",
        )
    )
    return True


def insertAlice(host, port):
    mustOk(
        tcpQuery(
            host,
            port,
            'INSERT INTO myapp.users (id,name,active,born,createdAt,avatar) VALUES (1,"alice",true,"2026-02-18","2026-02-18T12:34:56.123Z",0x01020304);',
        )
    )


def assertAliceRow(row):
    assert row["id"] == 1
    assert row["name"] == "alice"
    assert row["active"] is True
    assert row["born"] == "2026-02-18"
    assert row["createdAt"] == "2026-02-18T12:34:56.123Z"
    assert base64.b64decode(row["avatar"].encode("ascii")) == b"\x01\x02\x03\x04"


def startServer(repoRoot, configPath):
    exe = os.environ.get("BLAZEDB_EXECUTABLE", "./build/blazedbd")
    proc = subprocess.Popen(
        [exe, "--config", configPath],
        cwd=repoRoot,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    waitForListening(proc)
    return proc


def stopServer(proc):
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


def testPing(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        r = mustOk(tcpQuery("127.0.0.1", port, "PING;"))
        assert r["result"] == "PONG"
    finally:
        stopServer(proc)


def testCreateKeyspaceIfNotExists(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        mustOk(tcpQuery("127.0.0.1", port, "CREATE KEYSPACE IF NOT EXISTS myapp;"))
        mustOk(tcpQuery("127.0.0.1", port, "CREATE KEYSPACE IF NOT EXISTS myapp;"))
    finally:
        stopServer(proc)


def testCreateTableIfNotExists(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        ensureSchema("127.0.0.1", port)
        ensureSchema("127.0.0.1", port)
    finally:
        stopServer(proc)


def testInsertSelectTypedRow(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        ensureSchema("127.0.0.1", port)
        insertAlice("127.0.0.1", port)
        r = mustOk(tcpQuery("127.0.0.1", port, "SELECT * FROM myapp.users WHERE id=1;"))
        assert r["found"] is True
        assertAliceRow(r["row"])
        r = mustOk(tcpQuery("127.0.0.1", port, "SELECT name,avatar FROM myapp.users WHERE id=1;"))
        assert r["found"] is True
        assert r["row"]["name"] == "alice"
        assert base64.b64decode(r["row"]["avatar"].encode("ascii")) == b"\x01\x02\x03\x04"
    finally:
        stopServer(proc)


def testFlushRestartPersists(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)

    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        ensureSchema("127.0.0.1", port)
        insertAlice("127.0.0.1", port)
        mustOk(tcpQuery("127.0.0.1", port, "FLUSH myapp.users;"))
    finally:
        stopServer(proc)

    port2 = pickFreePort()
    cfg2 = tmp_path / "settings2.yml"
    writeConfig(str(cfg2), port2, str(dataDir))

    proc2 = startServer(repoRoot, str(cfg2))
    try:
        r = mustOk(tcpQuery("127.0.0.1", port2, "SELECT * FROM myapp.users WHERE id=1;"))
        assert r["found"] is True
        assertAliceRow(r["row"])
    finally:
        stopServer(proc2)


def testInsertMultiRow(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        mustOk(tcpQuery("127.0.0.1", port, "CREATE KEYSPACE IF NOT EXISTS multiRow;"))
        mustOk(
            tcpQuery(
                "127.0.0.1",
                port,
                "CREATE TABLE IF NOT EXISTS multiRow.people (id int64, name varchar, PRIMARY KEY (id));",
            )
        )
        mustOk(
            tcpQuery(
                "127.0.0.1",
                port,
                'INSERT INTO multiRow.people (id,name) VALUES (1,"a"), (2,"b");',
            )
        )

        r = mustOk(tcpQuery("127.0.0.1", port, "SELECT * FROM multiRow.people WHERE id=1;"))
        assert r["found"] is True
        assert r["row"]["id"] == 1
        assert r["row"]["name"] == "a"

        r = mustOk(tcpQuery("127.0.0.1", port, "SELECT * FROM multiRow.people WHERE id=2;"))
        assert r["found"] is True
        assert r["row"]["id"] == 2
        assert r["row"]["name"] == "b"
    finally:
        stopServer(proc)


def testDeleteByPk(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        mustOk(tcpQuery("127.0.0.1", port, "CREATE KEYSPACE IF NOT EXISTS deleteTest;"))
        mustOk(
            tcpQuery(
                "127.0.0.1",
                port,
                "CREATE TABLE IF NOT EXISTS deleteTest.items (id int64, name varchar, PRIMARY KEY (id));",
            )
        )
        mustOk(tcpQuery("127.0.0.1", port, 'INSERT INTO deleteTest.items (id,name) VALUES (1,"x");'))
        mustOk(tcpQuery("127.0.0.1", port, "DELETE FROM deleteTest.items WHERE id=1;"))

        r = mustOk(tcpQuery("127.0.0.1", port, "SELECT * FROM deleteTest.items WHERE id=1;"))
        assert r["found"] is False
    finally:
        stopServer(proc)


def testUpdatePreservesOtherColumns(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        ensureSchema("127.0.0.1", port)
        insertAlice("127.0.0.1", port)
        mustOk(tcpQuery("127.0.0.1", port, 'UPDATE myapp.users SET name="alice2" WHERE id=1;'))
        r = mustOk(tcpQuery("127.0.0.1", port, "SELECT * FROM myapp.users WHERE id=1;"))
        assert r["found"] is True
        row = r["row"]
        assert row["id"] == 1
        assert row["name"] == "alice2"
        assert row["active"] is True
        assert row["born"] == "2026-02-18"
        assert row["createdAt"] == "2026-02-18T12:34:56.123Z"
        assert base64.b64decode(row["avatar"].encode("ascii")) == b"\x01\x02\x03\x04"
    finally:
        stopServer(proc)


def testUpdateUpsertCreatesRow(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        ensureSchema("127.0.0.1", port)
        mustOk(tcpQuery("127.0.0.1", port, 'UPDATE myapp.users SET name="bob", active=false WHERE id=2;'))
        r = mustOk(tcpQuery("127.0.0.1", port, "SELECT * FROM myapp.users WHERE id=2;"))
        assert r["found"] is True
        row = r["row"]
        assert row["id"] == 2
        assert row["name"] == "bob"
        assert row["active"] is False
        assert row["born"] is None
        assert row["createdAt"] is None
        assert row["avatar"] is None
    finally:
        stopServer(proc)


def testUpdateSetNull(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        ensureSchema("127.0.0.1", port)
        insertAlice("127.0.0.1", port)
        mustOk(tcpQuery("127.0.0.1", port, "UPDATE myapp.users SET avatar=null WHERE id=1;"))
        r = mustOk(tcpQuery("127.0.0.1", port, "SELECT avatar FROM myapp.users WHERE id=1;"))
        assert r["found"] is True
        assert r["row"]["avatar"] is None
    finally:
        stopServer(proc)


def testUseKeyspaceUnqualified(tmp_path):
    repoRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataDir = tmp_path / "data"
    dataDir.mkdir(parents=True, exist_ok=True)
    port = pickFreePort()
    cfg = tmp_path / "settings.yml"
    writeConfig(str(cfg), port, str(dataDir))

    proc = startServer(repoRoot, str(cfg))
    try:
        res = tcpSession(
            "127.0.0.1",
            port,
            [
                "CREATE KEYSPACE IF NOT EXISTS sessionKs;",
                "USE sessionKs;",
                "CREATE TABLE IF NOT EXISTS sessionTable (id int64, name varchar, PRIMARY KEY (id));",
                'INSERT INTO sessionTable (id,name) VALUES (1,"alice"), (2,"bob");',
                "SELECT * FROM sessionTable WHERE id=1;",
                "SELECT * FROM sessionTable WHERE id=2;",
            ],
        )
        for i in range(4):
            mustOk(res[i])
        assert res[4]["ok"] is True and res[4]["found"] is True
        assert res[4]["row"]["id"] == 1
        assert res[4]["row"]["name"] == "alice"
        assert res[5]["ok"] is True and res[5]["found"] is True
        assert res[5]["row"]["id"] == 2
        assert res[5]["row"]["name"] == "bob"
    finally:
        stopServer(proc)
