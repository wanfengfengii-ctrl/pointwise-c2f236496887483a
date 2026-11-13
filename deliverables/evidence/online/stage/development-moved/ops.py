"""Packaging, user-service scaffold, and verify/demo orchestration."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import tempfile
import zipfile

import runtime

CODE = ("pipeline.py", "runtime.py", "ops.py", "checks.py", "lab.py")
MAIN = "from ops import main\nmain()\n"


def archive(items):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as target:
        for name, body in sorted(items.items()):
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            target.writestr(info, body)
    return buffer.getvalue()


def build(destination):
    source = Path(__file__).resolve().parent
    app = archive({**{name: (source / name).read_bytes() for name in CODE},
                   "__main__.py": MAIN.encode()})
    manifest = {"format": 1, "sha256": hashlib.sha256(app).hexdigest()}
    Path(destination).write_bytes(archive({"app.pyz": app,
                                           "manifest.json": runtime.encode(manifest)}))
    return manifest


def unpack(release):
    with zipfile.ZipFile(release) as package:
        if sorted(package.namelist()) != ["app.pyz", "manifest.json"]:
            raise ValueError("PACKAGE_LAYOUT")
        manifest = json.loads(package.read("manifest.json"))
        app = package.read("app.pyz")
    if manifest != {"format": 1, "sha256": hashlib.sha256(app).hexdigest()}:
        raise ValueError("PACKAGE_HASH")
    return app


def identity(root):
    return "local.sampling." + hashlib.sha256(str(Path(root).resolve()).encode()).hexdigest()[:16]


def agent_path(root):
    return Path.home() / "Library" / "LaunchAgents" / (identity(root) + ".plist")


def install(release, root, control):
    app = unpack(release)  # Verify before creating or changing ROOT.
    root = Path(root).resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError("INSTALL_TARGET_NOT_EMPTY")
    runtime.init(root, control)
    (root / "app.pyz").write_bytes(app)
    return {"root": str(root), "sha256": hashlib.sha256(app).hexdigest()}


def start(root):
    root = Path(root).resolve()
    control = Path(json.loads((root / "config.json").read_bytes())["control"])
    config = {
        "Label": identity(root),
        "ProgramArguments": [str(Path(sys.executable).resolve()), str(root / "app.pyz"),
                             "worker", str(root)],
        "WorkingDirectory": str(root),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 2,
        "StandardOutPath": str(control / "service.stdout"),
        "StandardErrorPath": str(control / "service.stderr"),
    }
    path = agent_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(config))
    domain = "gui/" + str(os.getuid())
    registered = subprocess.run(["launchctl", "print", domain + "/" + identity(root)],
                                capture_output=True).returncode == 0
    if not registered:
        subprocess.run(["launchctl", "bootstrap", domain, str(path)], check=True)
    subprocess.run(["launchctl", "kickstart", domain + "/" + identity(root)], check=True)
    return {"label": identity(root), "plist": str(path)}


def stop(root):
    target = "gui/" + str(os.getuid()) + "/" + identity(root)
    subprocess.run(["launchctl", "bootout", target], capture_output=True)
    agent_path(root).unlink(missing_ok=True)
    return {"stopped": identity(root)}


def fresh_directory(base):
    base = Path(base)
    candidate, counter = base, 2
    while candidate.exists():
        candidate = base.with_name(base.name + "-" + str(counter))
        counter += 1
    return candidate


def verify(directory):
    """Business regression plus physical full-disk online regression.

    Everything business-facing runs against the installed product, not the
    development checkout: the release is built here, installed by the lab
    driver, and exercised through ROOT/app.pyz.
    """
    import checks
    import lab
    base = fresh_directory(directory)
    base.mkdir(parents=True)
    release = base / "release.zip"
    manifest = build(release)
    normal = checks.normal()
    online = lab.verify_all(release, base / "online")
    return {"regression": "VERIFY_OK", "normal": normal["regression"],
            "online": online, "release_sha256": manifest["sha256"],
            "evidence": str(base.resolve())}


def demo(directory=None):
    """Install the release into a fresh directory and show one real batch."""
    import checks
    if directory is None:
        base = Path(tempfile.mkdtemp(prefix="sampling-demo-"))
    else:
        base = fresh_directory(directory)
        base.mkdir(parents=True)
    release = base / "release.zip"
    build(release)
    root, control = base / "root", base / "control"
    install(release, root, control)
    app = root / "app.pyz"
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)

    def call(*args):
        return json.loads(subprocess.run(
            [sys.executable, app, *[str(arg) for arg in args]],
            check=True, capture_output=True, text=True, env=env).stdout)

    batch = checks.small()
    fixture = base / "input.json"
    fixture.write_bytes(runtime.encode(batch))
    accepted = call("submit", root, fixture)
    processed = call("run", root)
    shown = call("read", root, batch["batch_id"])
    installed = checks.installed(root, batch)
    return {"demo": "DEMO_OK", "directory": str(base), "accepted": accepted,
            "processed": processed, "read": shown, "installed": installed}


def main(argv=None):
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(dest="operation", required=True)
    build_parser = subs.add_parser("build")
    build_parser.add_argument("release")
    install_parser = subs.add_parser("install")
    install_parser.add_argument("release")
    install_parser.add_argument("root")
    install_parser.add_argument("--control", required=True)
    for name in ("start", "stop", "status", "run", "worker"):
        subs.add_parser(name).add_argument("root")
    for name, argument in (("submit", "input"), ("read", "batch_id")):
        item = subs.add_parser(name)
        item.add_argument("root")
        item.add_argument(argument)
    verify_parser = subs.add_parser("verify")
    verify_parser.add_argument("directory", nargs="?", default="verify-evidence")
    demo_parser = subs.add_parser("demo")
    demo_parser.add_argument("directory", nargs="?", default=None)
    subs.add_parser("selftest")
    args = parser.parse_args(argv)
    if args.operation == "build":
        value = build(args.release)
    elif args.operation == "install":
        value = install(args.release, args.root, args.control)
    elif args.operation == "start":
        value = start(args.root)
    elif args.operation == "stop":
        value = stop(args.root)
    elif args.operation == "status":
        value = runtime.status(args.root)
    elif args.operation == "submit":
        value = runtime.submit(args.root, json.loads(Path(args.input).read_bytes()))
    elif args.operation == "read":
        value = runtime.read(args.root, args.batch_id)
    elif args.operation == "run":
        value = {"processed": runtime.run_one(args.root)}
    elif args.operation == "worker":
        runtime.worker(args.root)
        return
    elif args.operation == "verify":
        value = verify(args.directory)
    elif args.operation == "demo":
        value = demo(args.directory)
    else:
        import checks
        value = checks.normal()
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
