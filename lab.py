"""macOS physical-volume test driver. Tests policy; does not implement recovery."""
import argparse
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
import zipfile

import checks
import ops
import pipeline


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


def create(directory):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    image, mount = directory / "data.dmg", directory / "volume"
    mount.mkdir()
    command(["hdiutil", "create", "-size", "128m", "-fs", "HFS+",
             "-volname", "SamplingLab", "-format", "UDRW", image])
    command(["hdiutil", "attach", image, "-mountpoint", mount, "-nobrowse", "-owners", "on"])
    return mount


def fill(mount):
    # Confined to a private image. Never fills the host startup volume.
    mount = Path(mount).resolve()
    if not (mount.parent / "data.dmg").is_file() or not os.path.ismount(mount):
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


def case(release_path, directory, phase):
    directory = Path(directory).resolve()
    mount = create(directory)
    root, control = mount / "service", directory / "control"
    evidence = {"phase": phase, "source_removed": False}
    installed = False
    try:
        release_copy = detached_build(release_path, directory)
        evidence["source_removed"] = not (directory / "development").exists()
        ops.install(release_copy, root, control)
        installed = True
        app = root / "app.pyz"
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)

        def call(*args):
            return json.loads(command([sys.executable, app, *args], cwd=directory, env=env))

        batch = checks.small() if phase == "normal" else checks.large()
        fixture = directory / "input.json"
        fixture.write_bytes(pipeline.encode(batch))
        accepted = call("submit", root, fixture)
        evidence["accepted"] = accepted
        if phase != "normal":
            (control / "hook.json").write_bytes(pipeline.encode({"phase": phase}))
        ops.start(root)
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

            target = "gui/" + str(os.getuid()) + "/" + ops.identity(root)

            def pid():
                text = command(["launchctl", "print", target])
                match = re.search(r"\bpid = ([0-9]+)", text)
                return int(match[1]) if match else None

            before = wait_for(pid, "SERVICE_PID_MISSING")
            command(["launchctl", "kill", "SIGKILL", target])
            evidence["killed_pid"] = before
            evidence["restarted_pid"] = wait_for(
                lambda: (value if (value := pid()) and value != before else None),
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
            ops.stop(root)
        # Preserve the image and evidence, but always release the mount and service.
        subprocess.run(["hdiutil", "detach", str(mount)], capture_output=True)
        (directory / "evidence.json").write_bytes(pipeline.encode(evidence))


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
        base = Path(args.directory).resolve()
        base.mkdir(parents=True, exist_ok=False)
        records = [case(args.release, base / phase, phase)
                   for phase in ("normal", "stage", "result", "ledger")]
        (base / "evidence.json").write_bytes(pipeline.encode(records))
        print(json.dumps({"regression": "ONLINE_REGRESSION_OK",
                          "cases": [r["phase"] for r in records]}, sort_keys=True))


if __name__ == "__main__":
    main()
