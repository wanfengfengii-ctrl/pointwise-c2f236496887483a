"""Manual runner and unattended worker, sharing the existing kernel.

Disk-full recovery policy: when the data volume runs out of space while
staging the intermediate payload, writing the result file, or committing
the ledger transaction, the active batch is parked in the durable
``paused_space`` state, partial payloads are removed, and the worker
retries automatically until enough space is freed.  A reserve file kept
on the same data volume guarantees room for the pause bookkeeping on an
otherwise full disk.  Only ENOSPC and SQLITE_FULL (including extended
codes) count as space faults; any other failure keeps propagating and is
never reported as a space pause.  Recovery state lives only in ROOT.
"""
import errno
import fcntl
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

from pipeline import encode, receipt, stage, summarize, validate


SCHEMA = """
CREATE TABLE jobs(
 id TEXT PRIMARY KEY, digest TEXT NOT NULL, state TEXT NOT NULL,
 receipt TEXT, result_path TEXT
);
CREATE TABLE ledger(id TEXT PRIMARY KEY, result BLOB NOT NULL, receipt TEXT NOT NULL);
"""

ACTIVE = ("pending", "running", "paused_space")
PAUSED = "paused_space"
# Room kept on the data volume so the pause marker always fits on a full disk.
RESERVE_BYTES = 4 * 1024 * 1024
RESERVE_NAME = ".reserve"
PAUSE_RETRY_SECONDS = 2.0


@contextmanager
def connect(root):
    db = sqlite3.connect(str(Path(root) / "state.sqlite"), timeout=5)
    try:
        db.execute("PRAGMA synchronous=FULL")
        with db:
            yield db
    finally:
        db.close()


def init(root, control):
    root = Path(root).resolve()
    control = Path(control).resolve()
    root.mkdir(parents=True, exist_ok=True)
    control.mkdir(parents=True, exist_ok=True)
    for directory in ("inputs", "work", "results"):
        (root / directory).mkdir(exist_ok=True)
    with connect(root) as db:
        db.executescript(SCHEMA)
    (root / "config.json").write_bytes(encode({"control": str(control)}))
    ensure_reserve(root)


def barrier(root, phase):
    control = Path(json.loads((Path(root) / "config.json").read_bytes())["control"])
    plan = control / "hook.json"
    if not plan.exists() or json.loads(plan.read_bytes()).get("phase") != phase:
        return
    spent = control / (phase + ".spent")
    if spent.exists():
        return
    (control / (phase + ".ready")).write_text(str(os.getpid()))
    while not (control / (phase + ".go")).exists():
        time.sleep(0.05)
    spent.write_text("passed")


def sync_directory(directory):
    fd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def is_space_error(error):
    """Only ENOSPC and SQLITE_FULL (with extended codes) are space faults."""
    if isinstance(error, OSError):
        return error.errno == errno.ENOSPC
    if isinstance(error, sqlite3.Error):
        code = getattr(error, "sqlite_errorcode", None)
        return code is not None and code & 0xFF == sqlite3.SQLITE_FULL
    return False


def reserve_path(root):
    return Path(root) / "work" / RESERVE_NAME


def ensure_reserve(root):
    """Keep the space reserve on the data volume; recreated whenever missing."""
    path = reserve_path(root)
    if path.exists():
        return
    temporary = path.with_name(RESERVE_NAME + ".tmp")
    chunk = b"\0" * (1024 * 1024)
    with temporary.open("wb") as target:
        for _ in range(RESERVE_BYTES // len(chunk)):
            target.write(chunk)
        target.flush()
        os.fsync(target.fileno())
    os.replace(temporary, path)
    sync_directory(path.parent)


def submit(root, batch):
    root = Path(root)
    validate(batch)
    body = encode(batch)
    digest = hashlib.sha256(body).hexdigest()
    name = batch["batch_id"]
    with connect(root) as db:
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT digest FROM jobs WHERE id=?", (name,)).fetchone()
        if old:
            if old[0] != digest:
                raise ValueError("BATCH_CONFLICT")
            return {"accepted": name, "input_sha256": digest}
        if db.execute("SELECT 1 FROM jobs WHERE state != 'completed'").fetchone():
            raise ValueError("ACTIVE_BATCH")
        destination = root / "inputs" / (name + ".json")
        temporary = destination.with_suffix(".incoming")
        with temporary.open("wb") as target:
            target.write(body)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, destination)
        sync_directory(destination.parent)
        db.execute("INSERT INTO jobs(id,digest,state) VALUES(?,?,'pending')", (name, digest))
    return {"accepted": name, "input_sha256": digest}


def commit_result(db, name, result, ack, path):
    # Existing atomic accounting boundary, intentionally separate from file I/O.
    with db:
        db.execute("INSERT INTO ledger VALUES(?,?,?)",
                   (name, result, encode(ack).decode()))
        db.execute("UPDATE jobs SET state='completed',receipt=?,result_path=? WHERE id=?",
                   (encode(ack).decode(), path, name))


def write_file(path, writer):
    with path.open("wb") as target:
        writer(target)
        target.flush()
        os.fsync(target.fileno())


def process(root, name, digest):
    """Replayable pipeline for one accepted batch; derived files are atomic."""
    root = Path(root)
    batch = json.loads((root / "inputs" / (name + ".json")).read_bytes())
    intermediate = root / "work" / (name + ".jsonl")
    barrier(root, "stage")
    temporary = intermediate.with_name(intermediate.name + ".partial")
    write_file(temporary, lambda target: stage(batch, target))
    os.replace(temporary, intermediate)
    sync_directory(intermediate.parent)
    with intermediate.open("rb") as source:
        result = encode(summarize(source))
    destination = root / "results" / (name + ".json")
    barrier(root, "result")
    temporary = destination.with_name(destination.name + ".partial")
    write_file(temporary, lambda target: target.write(result))
    os.replace(temporary, destination)
    sync_directory(destination.parent)
    ack = receipt(name, digest, result)
    barrier(root, "ledger")
    with connect(root) as db:
        commit_result(db, name, result, ack, str(destination.relative_to(root)))


def pause(root, name):
    """Durably record the space pause; never touches results, ledger, or input."""
    root = Path(root)
    # Free bookkeeping room on the full volume before writing the marker.
    reserve_path(root).unlink(missing_ok=True)
    for partial in (root / "work" / (RESERVE_NAME + ".tmp"),
                    root / "work" / (name + ".jsonl.partial"),
                    root / "results" / (name + ".json.partial")):
        partial.unlink(missing_ok=True)
    try:
        with connect(root) as db:
            db.execute("UPDATE jobs SET state=? WHERE id=? AND state != 'completed'",
                       (PAUSED, name))
    except Exception as error:
        if not is_space_error(error):
            raise
        # No room even for the marker; the next attempt records the pause again.


def attempt(root):
    """Drive the single active batch once; returns 'done', 'paused', or None."""
    root = Path(root)
    with connect(root) as db:
        job = db.execute(
            "SELECT id,digest,state FROM jobs WHERE state IN ('pending','running','paused_space')"
            " ORDER BY id LIMIT 1").fetchone()
    if job is None:
        return None
    name, digest, state = job
    try:
        if state == "pending":
            with connect(root) as db:
                db.execute("UPDATE jobs SET state='running' WHERE id=?", (name,))
        ensure_reserve(root)
        process(root, name, digest)
    except Exception as error:
        if not is_space_error(error):
            raise
        pause(root, name)
        return "paused"
    try:
        ensure_reserve(root)  # Top up for the next batch; failure pauses that batch later.
    except OSError:
        pass
    return "done"


def run_one(root):
    return attempt(root) == "done"


def status(root):
    with connect(root) as db:
        rows = db.execute("SELECT id,state,receipt FROM jobs ORDER BY id").fetchall()
    return [{"batch_id": name, "state": state,
             "receipt": json.loads(ack) if ack is not None else None}
            for name, state, ack in rows]


def read(root, name):
    with connect(root) as db:
        row = db.execute("SELECT state,result_path,receipt FROM jobs WHERE id=?", (name,)).fetchone()
    if row is None or row[0] != "completed":
        return {"batch_id": name, "available": False}
    body = (Path(root) / row[1]).read_bytes()
    ack = json.loads(row[2])
    if hashlib.sha256(body).hexdigest() != ack["result_sha256"]:
        raise ValueError("RESULT_CORRUPT")
    return {"batch_id": name, "available": True, "result": json.loads(body), "receipt": ack}


def worker(root):
    root = Path(root)
    with (root / "worker.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            outcome = attempt(root)
            if outcome == "paused":
                time.sleep(PAUSE_RETRY_SECONDS)
            elif outcome is None:
                time.sleep(0.2)
