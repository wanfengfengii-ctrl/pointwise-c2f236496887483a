"""Physical-volume test driver. Tests policy; does not implement recovery.

On macOS the isolated volume is a 128 MiB HFS+ image created with hdiutil
and the worker is supervised by the per-user LaunchAgent.  On Linux the
driver uses a small dedicated tmpfs (such as /dev/shm) and a minimal
KeepAlive-style supervisor, so the same real-ENOSPC boundaries are
exercised without ever filling the host disk.  All assertions are
identical on both platforms.
"""
import argparse
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import zipfile

import checks
import ops
import pipeline

DARWIN = sys.platform == "darwin"


def command(argv, **kwargs):
    return subprocess.run([str(x) for x in argv], check=True, capture_output=True,
                          text=True, **kwargs).stdout


def wait_for(predicate, message, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError(message)


def linux_mount(directory):
    """A fresh directory on a small dedicated tmpfs; refuses anything else."""
    shm = Path("/dev/shm")
    mounts = Path("/proc/mounts").read_bytes().decode()
    if not any(len(fields) >= 3 and fields[1] == "/dev/shm" and fields[2] == "tmpfs"
               for fields in (line.split() for line in mounts.splitlines())):
        raise ValueError("NO_ISOLATED_VOLUME")
    stats = os.statvfs(shm)
    total = stats.f_blocks * stats.f_frsize
    if not 32 * 1024 * 1024 <= total <= 256 * 1024 * 1024:
        # Only ever fill a small dedicated scratch volume, never a large share.
        raise ValueError("UNSAFE_VOLUME_SIZE")
    for stale in shm.glob("sampling-lab-*"):
        try:
            pid = int(stale.name.split("-")[2])
        except (IndexError, ValueError):
            continue
        if not Path("/proc/%d" % pid).exists():
            shutil.rmtree(stale, ignore_errors=True)
    mount = shm / ("sampling-lab-%d-%s" % (os.getpid(), directory.name))
    mount.mkdir()
    return mount


def create(directory):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    if not DARWIN:
        return linux_mount(directory)
    image, mount = directory / "data.dmg", directory / "volume"
    mount.mkdir()
    command(["hdiutil", "create", "-size", "128m", "-fs", "HFS+",
             "-volname", "SamplingLab", "-format", "UDRW", image])
    command(["hdiutil", "attach", image, "-mountpoint", mount, "-nobrowse", "-owners", "on"])
    return mount


def fill(mount):
    # Confined to a private volume. Never fills the host startup volume.
    mount = Path(mount).resolve()
    if DARWIN:
        if not (mount.parent / "data.dmg").is_file() or not os.path.ismount(mount):
            raise ValueError("NOT_LAB_VOLUME")
    else:
        shm = Path("/dev/shm").resolve()
        if mount.parent != shm or os.stat(mount).st_dev != os.stat(shm).st_dev:
            raise ValueError("NOT_LAB_VOLUME")
    total = 0
    block = b"x" * 65536
    with (mount / "ballast").open("wb", buffering=0) as target:
        for size in (65536, 4096, 1):
            while True:
                try:
                    written = target.write(block[:size])
                    if written is None or written == 0:
                        raise OSError(errno.ENOSPC, "no progress")
                    total += written
                except OSError as error:
                    if error.errno != errno.ENOSPC:
                        raise
                    break
        try:
            os.fsync(target.fileno())
        except OSError as error:
            if error.errno != errno.ENOSPC:
                raise
    return {"errno": errno.ENOSPC, "ballast_bytes": total}


def release(mount, megabytes=48):
    path = Path(mount) / "ballast"
    with path.open("r+b") as target:
        target.truncate(max(0, path.stat().st_size - megabytes * 1024 * 1024))
        target.flush()
        os.fsync(target.fileno())


def detached_build(release_path, directory):
    app = ops.unpack(release_path)
    development = directory / "development"
    development.mkdir()
    with zipfile.ZipFile(io.BytesIO(app)) as package:
        for name in ops.CODE:
            (development / name).write_bytes(package.read(name))
    built = directory / "release.zip"
    command([sys.executable, development / "ops.py", "build", built], cwd=directory)
    assert built.read_bytes() == Path(release_path).read_bytes(), "NONREPRODUCIBLE_PACKAGE"
    development.rename(directory / "development-moved")
    return built


class LaunchAgentService:
    """macOS per-user agent supervision (existing behavior)."""

    def __init__(self, root):
        self.root = root
        self.target = "gui/" + str(os.getuid()) + "/" + ops.identity(root)

    def start(self):
        ops.start(self.root)

    def pid(self):
        text = command(["launchctl", "print", self.target])
        match = re.search(r"\bpid = ([0-9]+)", text)
        return int(match[1]) if match else None

    def kill(self):
        command(["launchctl", "kill", "SIGKILL", self.target])

    def stop(self):
        ops.stop(self.root)


class Supervisor:
    """Minimal KeepAlive-style restarter where launchd is unavailable."""

    def __init__(self, argv, control, env, cwd):
        self.argv = [str(arg) for arg in argv]
        self.control = Path(control)
        self.env = env
        self.cwd = cwd
        self.child = None
        self.halted = False
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        while not self.halted:
            if self.child is None or self.child.poll() is not None:
                with (self.control / "service.stdout").open("ab") as out, \
                        (self.control / "service.stderr").open("ab") as err:
                    self.child = subprocess.Popen(self.argv, stdout=out, stderr=err,
                                                  env=self.env, cwd=self.cwd)
                time.sleep(1.0)  # Restart throttle, mirroring ThrottleInterval.
            else:
                time.sleep(0.05)

    def pid(self):
        if self.child is not None and self.child.poll() is None:
            return self.child.pid
        return None

    def kill(self):
        if self.child is not None and self.child.poll() is None:
            self.child.send_signal(signal.SIGKILL)

    def stop(self):
        self.halted = True
        if self.thread is not None:
            self.thread.join(timeout=10)
        child = self.child
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


class SupervisorService:
    """Linux stand-in for the LaunchAgent with the same KeepAlive contract."""

    def __init__(self, app, root, control, env, cwd):
        self.supervisor = Supervisor([sys.executable, app, "worker", root],
                                     control, env, cwd)

    def start(self):
        self.supervisor.start()

    def pid(self):
        return self.supervisor.pid()

    def kill(self):
        self.supervisor.kill()

    def stop(self):
        self.supervisor.stop()


def case(release_path, directory, phase):
    directory = Path(directory).resolve()
    mount = create(directory)
    root, control = mount / "service", directory / "control"
    evidence = {"phase": phase, "source_removed": False}
    installed = False
    service = None
    try:
        release_copy = detached_build(release_path, directory)
        evidence["source_removed"] = not (directory / "development").exists()
        ops.install(release_copy, root, control)
        installed = True
        app = root / "app.pyz"
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        service = (LaunchAgentService(root) if DARWIN
                   else SupervisorService(app, root, control, env, directory))

        def call(*args):
            return json.loads(command([sys.executable, app, *args], cwd=directory, env=env))

        batch = checks.small() if phase == "normal" else checks.large()
        fixture = directory / "input.json"
        fixture.write_bytes(pipeline.encode(batch))
        accepted = call("submit", root, fixture)
        evidence["accepted"] = accepted
        if phase != "normal":
            (control / "hook.json").write_bytes(pipeline.encode({"phase": phase}))
        service.start()
        if phase != "normal":
            wait_for(lambda: (control / (phase + ".ready")).exists(), "BOUNDARY_NOT_REACHED")
            evidence["physical_full"] = fill(mount)
            (control / (phase + ".go")).write_text("continue")

            def paused():
                try:
                    state = call("status", root)
                    return state if state and state[0]["state"] == "paused_space" else None
                except (subprocess.CalledProcessError, json.JSONDecodeError):
                    return None

            evidence["paused"] = wait_for(paused, "SPACE_PAUSE_MISSING")
            assert evidence["paused"][0]["receipt"] is None, "FALSE_SUCCESS_RECEIPT"
            assert call("read", root, batch["batch_id"])["available"] is False, "PARTIAL_VISIBLE"
            saved = (root / "inputs" / (batch["batch_id"] + ".json")).read_bytes()
            assert hashlib.sha256(saved).hexdigest() == accepted["input_sha256"], "INPUT_LOST"
            db = sqlite3.connect("file:" + str(root / "state.sqlite") + "?mode=ro", uri=True)
            try:
                assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (0,), "PARTIAL_LEDGER"
            finally:
                db.close()

            before = wait_for(service.pid, "SERVICE_PID_MISSING")
            service.kill()
            evidence["killed_pid"] = before
            evidence["restarted_pid"] = wait_for(
                lambda: (value if (value := service.pid()) and value != before else None),
                "LAUNCHAGENT_DID_NOT_RESTART")
            # Still full at restart; status must remain recoverable before we release bytes.
            evidence["after_restart"] = wait_for(paused, "PAUSE_LOST_ON_RESTART")
            release(mount)

        def completed():
            try:
                value = call("status", root)
                return value if value and value[0]["state"] == "completed" else None
            except (subprocess.CalledProcessError, json.JSONDecodeError):
                return None

        evidence["completed"] = wait_for(completed, "DID_NOT_RESUME", seconds=60)
        # Use package's business oracle, not host runtime imports.
        evidence["business"] = json.loads(command(
            [sys.executable, app, "selftest"], cwd=directory, env=env))
        evidence["installed"] = json.loads(command(
            [sys.executable, "-c",
             "import runpy,sys; sys.path.insert(0,sys.argv[1]); "
             "import checks,json; print(json.dumps(checks.installed(sys.argv[2],"
             "json.load(open(sys.argv[3])))))", app, root, fixture],
            cwd=directory, env=env))
        return evidence
    finally:
        if installed:
            service.stop()
        if DARWIN:
            # Preserve the image and evidence, but always release the mount and service.
            subprocess.run(["hdiutil", "detach", str(mount)], capture_output=True)
        else:
            shutil.rmtree(mount, ignore_errors=True)
        (directory / "evidence.json").write_bytes(pipeline.encode(evidence))


def verify_all(release, directory):
    base = Path(directory).resolve()
    base.mkdir(parents=True, exist_ok=False)
    records = [case(release, base / phase, phase)
               for phase in ("normal", "stage", "result", "ledger")]
    (base / "evidence.json").write_bytes(pipeline.encode(records))
    return {"regression": "ONLINE_REGRESSION_OK",
            "cases": [record["phase"] for record in records]}


def main():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(dest="operation", required=True)
    create_parser = subs.add_parser("create")
    create_parser.add_argument("directory")
    for name in ("fill", "release"):
        subs.add_parser(name).add_argument("mount")
    for name in ("case", "verify"):
        item = subs.add_parser(name)
        item.add_argument("release")
        item.add_argument("directory")
        if name == "case":
            item.add_argument("phase", choices=("normal", "stage", "result", "ledger"))
    args = parser.parse_args()
    if args.operation == "create":
        print(create(args.directory))
    elif args.operation == "fill":
        print(json.dumps(fill(args.mount)))
    elif args.operation == "release":
        release(args.mount)
    elif args.operation == "case":
        print(json.dumps(case(args.release, args.directory, args.phase), sort_keys=True))
    else:
        print(json.dumps(verify_all(args.release, args.directory), sort_keys=True))


if __name__ == "__main__":
    main()
