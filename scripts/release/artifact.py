"""Build once, package final bytes, smoke extraction, then accept exactly four candidates."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib

FEATURES = ["s3", "ipfs", "swisstransfer", "webdav", "github"]
TARGETS = {
    "x86_64-unknown-linux-gnu": "ubuntu-24.04",
    "aarch64-unknown-linux-gnu": "ubuntu-24.04-arm",
    "x86_64-apple-darwin": "macos-15-intel",
    "aarch64-apple-darwin": "macos-15",
}
CASES = {
    "help_version", "checked_http", "required_mismatch", "incumbent_preserved",
    "json_units", "payload_isolation", "native_tty", "interruption",
    "offline_info", "range_restoration", "redirect_refused", "head_denied",
    "ignored_range_refused",
}
MEMBERS = {"xget", "LICENSE-MIT", "LICENSE-APACHE", "INSTALL.txt", "build.json"}
ROOT = Path(__file__).resolve().parents[2]
MAX_ARCHIVE = 512 * 1024 * 1024


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--target", choices=TARGETS, required=True)
    build.add_argument("--output", type=Path, required=True)
    aggregate = commands.add_parser("aggregate")
    aggregate.add_argument("--input", type=Path, required=True)
    aggregate.add_argument("--ci", type=Path, required=True)
    aggregate.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "build":
        build_candidate(args.target, args.output)
    else:
        aggregate_candidates(args.input, args.ci, args.output)


def run(*command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
        raise ValueError(f"invalid or oversized JSON file: {path}")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique)


def source_identity():
    sha = run("git", "rev-parse", "HEAD")
    if sha != os.environ["GITHUB_SHA"]:
        raise ValueError("checkout is not the requested workflow source")
    run("git", "diff", "--exit-code", "HEAD", "--")
    root = tomllib.loads((ROOT / "Cargo.toml").read_text())
    cli = tomllib.loads((ROOT / "cli/Cargo.toml").read_text())
    version = cli["package"]["version"]
    if root["package"]["version"] != version or not re.fullmatch(r"0\.2\.0-rc\.[1-9][0-9]*", version):
        raise ValueError("candidate requires aligned 0.2.0-rc.N package versions")
    return {
        "sha": sha, "tree": run("git", "rev-parse", "HEAD^{tree}"),
        "lock_sha256": digest(ROOT / "Cargo.lock"), "version": version,
        "repository": os.environ["GITHUB_REPOSITORY"],
        "workflow_run_id": os.environ["GITHUB_RUN_ID"],
        "workflow_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
        "workflow_ref": os.environ["GITHUB_WORKFLOW_REF"],
    }


def build_command(target):
    return ["cargo", "+1.97.1", "build", "--release", "--locked", "--package", "xget-cli",
            "--bin", "xget", "--features", " ".join(FEATURES), "--target", target]


def git_dependencies(metadata):
    dependencies = []
    for package in metadata["packages"]:
        source = package["source"]
        if source is None:
            if Path(package["manifest_path"]).resolve() not in {ROOT / "Cargo.toml", ROOT / "cli/Cargo.toml"}:
                raise ValueError("metadata contains a non-workspace path dependency")
        elif source.startswith("git+"):
            if not re.search(r"#[0-9a-f]{40}$", source):
                raise ValueError("git dependency has no resolved revision")
            dependencies.append({"name": package["name"], "version": package["version"], "source": source})
    if not {"xbytes", "xprogress"} <= {item["name"] for item in dependencies}:
        raise ValueError("expected locked Git siblings are missing from metadata")
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    locked = {(p["name"], p["version"], p.get("source")) for p in lock["package"]}
    if any((p["name"], p["version"], p["source"]) not in locked for p in dependencies):
        raise ValueError("actual Git metadata does not match Cargo.lock")
    return sorted(dependencies, key=lambda p: (p["name"], p["version"], p["source"]))


def require_empty(path):
    if path.exists() or path.is_symlink():
        raise ValueError(f"use a new empty directory: {path}")
    path.mkdir(parents=True)


def build_candidate(target, output):
    source = source_identity()
    # An isolated Cargo home and no ancestor config prevent the checkout's sibling path overrides.
    home = Path(os.environ["CARGO_HOME"]).resolve()
    target_dir = Path(os.environ["CARGO_TARGET_DIR"]).resolve()
    temporary = Path(os.environ["RUNNER_TEMP"]).resolve()
    for path in (home, target_dir, output.resolve()):
        if not path.is_relative_to(temporary) or path.is_relative_to(ROOT):
            raise ValueError("candidate outputs and Cargo directories must be outside checkout in RUNNER_TEMP")
    for directory in (ROOT, *ROOT.parents, home.parent):
        for name in ("config", "config.toml"):
            if (directory / ".cargo" / name).exists():
                raise ValueError("Cargo configuration override is not allowed")
    if home.exists() and any(home.iterdir()):
        raise ValueError("candidate Cargo home must start empty")
    forbidden = [key for key in os.environ if key.startswith(("CARGO_PROFILE_", "CARGO_ENCODED_", "CARGO_TARGET_"))
                 and key != "CARGO_TARGET_DIR"]
    if forbidden or any(os.environ.get(key) for key in ("RUSTFLAGS", "RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_ENCODED_RUSTFLAGS")):
        raise ValueError("unexpected compiler or Cargo overrides")
    if os.environ.get("CARGO_INCREMENTAL") != "0" or os.environ.get("CARGO_BUILD_JOBS") != "2":
        raise ValueError("candidate requires two build jobs and no incremental compilation")
    require_empty(output)
    metadata = json.loads(run("cargo", "+1.97.1", "metadata", "--locked", "--format-version", "1",
                              "--features", "xget-cli/" + ",xget-cli/".join(FEATURES)))
    dependencies = git_dependencies(metadata)
    command = build_command(target)
    subprocess.run(command, cwd=ROOT, check=True)
    # Check the lock and source again after Cargo; an unlocked resolution is not candidate provenance.
    if source_identity() != source:
        raise ValueError("source changed during build")
    binary = target_dir / target / "release/xget"
    tools = {"rustc": run("rustc", "+1.97.1", "-Vv"), "cargo": run("cargo", "+1.97.1", "-V")}
    if not tools["rustc"].startswith("rustc 1.97.1 ") or "host: " + target not in tools["rustc"]:
        raise ValueError("build is not using the pinned native toolchain")
    image = {key: os.environ.get(key, "") for key in ("ImageOS", "ImageVersion", "RUNNER_OS", "RUNNER_ARCH")}
    if not image["ImageOS"] or not image["ImageVersion"] or os.environ["CANDIDATE_RUNNER"] != TARGETS[target]:
        raise ValueError("runner image identity is missing or mismatched")
    inspection = {"file": run("file", str(binary))}
    architecture = {"x86_64-unknown-linux-gnu": "x86-64", "aarch64-unknown-linux-gnu": "aarch64",
                    "x86_64-apple-darwin": "x86_64", "aarch64-apple-darwin": "arm64"}[target]
    if architecture not in inspection["file"]:
        raise ValueError("final binary architecture does not match target")
    if "linux" in target:
        inspection["dynamic"] = run("readelf", "-d", str(binary))
        inspection["symbol_versions"] = run("readelf", "--version-info", str(binary))
    else:
        inspection["load_commands"] = run("otool", "-l", str(binary))
        inspection["dynamic"] = run("otool", "-L", str(binary))
    build = {
        "schema_version": 1, "source": source, "target": target, "features": FEATURES,
        "build_command": command, "tools": tools, "git_dependencies": dependencies,
        "runner": TARGETS[target], "runner_image": image, "binary_sha256": digest(binary),
        "binary_inspection": inspection, "strip": "not performed",
    }
    stem = f"xget-{source['version']}-{target}"
    archive = output / (stem + ".tar.gz")
    with tempfile.TemporaryDirectory(prefix="candidate-package-", dir=temporary) as directory:
        staging = Path(directory)
        shutil.copyfile(binary, staging / "xget")
        (staging / "xget").chmod(0o755)
        for license_name in ("LICENSE-MIT", "LICENSE-APACHE"):
            shutil.copyfile(ROOT / license_name, staging / license_name)
        shutil.copyfile(ROOT / "scripts/release/INSTALL.txt", staging / "INSTALL.txt")
        write_json(staging / "build.json", build)
        with tarfile.open(archive, "w:gz", format=tarfile.USTAR_FORMAT) as bundle:
            for name in sorted(MEMBERS):
                bundle.add(staging / name, arcname=name, recursive=False)
    smoke = output / (stem + ".smoke.json")
    run_smoke_process([sys.executable, "-I", "-B", str(ROOT / "scripts/release/smoke.py"),
                       "--archive", str(archive), "--receipt", str(smoke), "--target", target,
                       "--version", source["version"], "--binary-sha256", build["binary_sha256"]],
                      output / (stem + ".timeout.json"))
    manifest = {"schema_version": 1, "build": build, "archive": file_record(archive), "smoke": file_record(smoke)}
    validate_manifest(manifest, source)
    write_json(output / (stem + ".manifest.json"), manifest)


def run_smoke_process(command, timeout_receipt, timeout=240, grace=5):
    """A timed-out smoke owns a dedicated group, including its CLI descendants."""
    process = subprocess.Popen(command, cwd=ROOT, start_new_session=True)
    try:
        result = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGINT)
        except ProcessLookupError:
            pass
        # Keep the leader unreaped through the grace period so its group ID cannot be reused.
        time.sleep(grace)
        forced = False
        try:
            os.killpg(process.pid, signal.SIGKILL)
            forced = True
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            pass
        write_json(timeout_receipt, {"schema_version": 1, "status": "failed",
                   "failure": "smoke process deadline exceeded", "deadline_seconds": timeout,
                   "cleanup_grace_seconds": grace, "owned_process_group": process.pid,
                   "forced_group_signal_sent": forced, "leader_exit_code": process.returncode,
                   "native_settlement": "not established by forced process cleanup"})
        raise
    if result != 0:
        raise subprocess.CalledProcessError(result, command)


def file_record(path):
    return {"name": path.name, "size": path.stat().st_size, "sha256": digest(path)}


def checked_file(directory, record):
    if (set(record) != {"name", "size", "sha256"}
            or not isinstance(record["name"], str) or not re.fullmatch(r"[a-zA-Z0-9._-]+", record["name"])
            or type(record["size"]) is not int or not 0 < record["size"] <= MAX_ARCHIVE
            or not isinstance(record["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", record["sha256"])):
        raise ValueError("invalid payload record")
    path = directory / record["name"]
    try:
        actual = path.lstat()
    except FileNotFoundError as error:
        raise ValueError(f"missing payload: {path}") from error
    if (not stat.S_ISREG(actual.st_mode) or actual.st_size != record["size"]
            or not 0 < actual.st_size <= MAX_ARCHIVE):
        raise ValueError(f"invalid payload type or size: {path}")
    if digest(path) != record["sha256"]:
        raise ValueError(f"corrupt payload: {path}")
    return path


def validate_manifest(manifest, source):
    if set(manifest) != {"schema_version", "build", "archive", "smoke"} or manifest["schema_version"] != 1:
        raise ValueError("invalid manifest schema")
    build = manifest["build"]
    target = build["target"]
    if target not in TARGETS or build["source"] != source or build["schema_version"] != 1:
        raise ValueError("manifest source or target mismatch")
    if build["features"] != FEATURES or build["runner"] != TARGETS[target] or build["build_command"] != build_command(target):
        raise ValueError("manifest build policy mismatch")
    if not re.fullmatch(r"[0-9a-f]{64}", build["binary_sha256"]):
        raise ValueError("invalid binary digest")
    if not build["tools"]["rustc"].startswith("rustc 1.97.1 ") or not build["tools"]["cargo"].startswith("cargo 1.97.1 "):
        raise ValueError("manifest toolchain mismatch")
    if "host: " + target not in build["tools"]["rustc"]:
        raise ValueError("manifest native host mismatch")
    if not build["runner_image"]["ImageOS"] or not build["runner_image"]["ImageVersion"] or not build["binary_inspection"]:
        raise ValueError("manifest platform evidence missing")
    dependencies = build["git_dependencies"]
    if not {"xbytes", "xprogress"} <= {p["name"] for p in dependencies}:
        raise ValueError("manifest Git dependencies missing")
    if any(not re.search(r"#[0-9a-f]{40}$", p["source"]) for p in dependencies):
        raise ValueError("manifest Git revision missing")
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    expected_git = sorted((p["name"], p["version"], p["source"]) for p in lock["package"]
                          if p.get("source", "").startswith("git+"))
    if sorted((p["name"], p["version"], p["source"]) for p in dependencies) != expected_git:
        raise ValueError("manifest Git revisions disagree with committed lockfile")
    stem = f"xget-{source['version']}-{target}"
    if manifest["archive"]["name"] != stem + ".tar.gz" or manifest["smoke"]["name"] != stem + ".smoke.json":
        raise ValueError("manifest payload filename mismatch")


def extract_archive(archive, destination):
    """Only the five flat regular members are admitted, before any extraction takes place."""
    if archive.stat().st_size > MAX_ARCHIVE or archive.is_symlink():
        raise ValueError("archive is oversized or linked")
    if not destination.is_dir() or any(destination.iterdir()) or destination.is_symlink():
        raise ValueError("extraction requires an existing new empty directory")
    preflight_ustar(archive)
    with tarfile.open(archive, "r:gz") as bundle:
        members = []
        names = set()
        total = 0
        for member in bundle:
            name = PurePosixPath(member.name)
            if (not member.isfile() or member.name not in MEMBERS or len(name.parts) != 1
                    or member.name in names or member.mode & 0o7000 or member.size < 0):
                raise ValueError("archive contains an unexpected, duplicate, linked or unsafe member")
            names.add(member.name)
            members.append(member)
            total += member.size
            if total > MAX_ARCHIVE or len(members) > len(MEMBERS):
                raise ValueError("archive exceeds extraction bounds")
        if names != MEMBERS:
            raise ValueError("archive members are missing")
        for member in members:
            with bundle.extractfile(member) as source, (destination / member.name).open("xb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
            (destination / member.name).chmod(0o755 if member.name == "xget" else 0o644)


def preflight_ustar(archive):
    """Admit only our builder's flat USTAR bytes before tarfile can consume extensions."""
    names = set()
    expanded = 0
    with gzip.open(archive, "rb") as stream:
        def read_bounded(size):
            nonlocal expanded
            block = stream.read(min(size, MAX_ARCHIVE - expanded + 1))
            expanded += len(block)
            if expanded > MAX_ARCHIVE:
                raise ValueError("expanded archive exceeds bound including headers and padding")
            return block

        while True:
            header = read_bounded(512)
            if len(header) != 512:
                raise ValueError("truncated USTAR header or end marker")
            if header == bytes(512):
                if read_bounded(512) != bytes(512):
                    raise ValueError("missing second USTAR end marker")
                while True:
                    padding = read_bounded(64 * 1024)
                    if not padding:
                        break
                    if any(padding):
                        raise ValueError("nonzero bytes after USTAR end markers")
                if names != MEMBERS:
                    raise ValueError("archive members are missing")
                return
            # PAX and GNU extensions are rejected from their raw type flag, before parser admission.
            if (header[156:157] not in (b"0", b"\0") or header[257:265] != b"ustar\00000"
                    or any(header[157:257]) or any(header[345:500])):
                raise ValueError("only flat regular USTAR headers are supported; extensions are refused")
            name = header[:100].split(b"\0", 1)[0]
            if name not in {member.encode("ascii") for member in MEMBERS} or header[:100] != name.ljust(100, b"\0"):
                raise ValueError("unexpected USTAR member name")
            decoded = name.decode("ascii")
            if decoded in names:
                raise ValueError("duplicate USTAR member")
            names.add(decoded)
            if ustar_octal(header[148:156]) != sum(header[:148]) + 8 * 32 + sum(header[156:]):
                raise ValueError("invalid USTAR header checksum")
            size = ustar_octal(header[124:136])
            if ustar_octal(header[100:108]) & 0o7000:
                raise ValueError("unsafe USTAR member mode")
            remaining = ((size + 511) // 512) * 512
            if remaining > MAX_ARCHIVE - expanded:
                raise ValueError("expanded archive exceeds bound including headers and padding")
            while remaining:
                block = read_bounded(min(remaining, 64 * 1024))
                if not block:
                    raise ValueError("truncated USTAR member body")
                remaining -= len(block)


def ustar_octal(field):
    digits = field.strip(b"\0 ")
    if not re.fullmatch(b"[0-7]+", digits):
        raise ValueError("invalid USTAR octal field")
    return int(digits, 8)


def validate_smoke(receipt, manifest):
    if (receipt["schema_version"] != 1 or receipt["status"] != "passed"
            or receipt["target"] != manifest["build"]["target"]
            or receipt["version"] != manifest["build"]["source"]["version"]
            or receipt["binary_sha256"] != manifest["build"]["binary_sha256"]
            or receipt["archive_sha256"] != manifest["archive"]["sha256"]
            or set(receipt["cases"]) != CASES
            or any(case.get("status") != "passed" for case in receipt["cases"].values())):
        raise ValueError("smoke evidence is missing, failed or mismatched")
    if receipt["cases"]["range_restoration"].get("restored_bytes", 0) <= 0:
        raise ValueError("smoke did not restore any recorded prefix")


def aggregate_candidates(inputs, ci_path, output):
    temporary = Path(os.environ["RUNNER_TEMP"]).resolve()
    if not output.resolve().is_relative_to(temporary) or output.resolve().is_relative_to(ROOT):
        raise ValueError("accepted candidate output must be outside checkout in RUNNER_TEMP")
    source = source_identity()
    ci = read_json(ci_path)
    if (ci["head_sha"] != source["sha"] or ci["repository"] != source["repository"]
            or ci["conclusion"] != "success" or ci["event"] != "push"
            or ci["path"] != ".github/workflows/ci.yml"
            or set(ci["jobs"]) != {"rustfmt (nightly)", "clippy", "test", "windows check"}
            or any(status != "success" for status in ci["jobs"].values())):
        raise ValueError("successful exact-source CI evidence is required")
    manifests = []
    payloads = []
    seen = set()
    expected_files = set()
    for path in sorted(inputs.glob("*/*.manifest.json")):
        if path.is_symlink() or path.parent.is_symlink():
            raise ValueError("linked input is not allowed")
        manifest = read_json(path)
        validate_manifest(manifest, source)
        target = manifest["build"]["target"]
        if (target in seen or path.parent.name != "candidate-" + target
                or path.name != f"xget-{source['version']}-{target}.manifest.json"):
            raise ValueError("duplicate or misplaced target")
        seen.add(target)
        archive = checked_file(path.parent, manifest["archive"])
        smoke_path = checked_file(path.parent, manifest["smoke"])
        validate_smoke(read_json(smoke_path), manifest)
        with tempfile.TemporaryDirectory(prefix="candidate-verify-", dir=os.environ["RUNNER_TEMP"]) as directory:
            extraction = Path(directory)
            extract_archive(archive, extraction)
            if read_json(extraction / "build.json") != manifest["build"] or digest(extraction / "xget") != manifest["build"]["binary_sha256"]:
                raise ValueError("archive internal metadata or binary disagrees with manifest")
            for name in ("LICENSE-MIT", "LICENSE-APACHE"):
                if digest(extraction / name) != digest(ROOT / name):
                    raise ValueError("archive license differs from source")
            if digest(extraction / "INSTALL.txt") != digest(ROOT / "scripts/release/INSTALL.txt"):
                raise ValueError("archive installation contract differs from source")
        manifests.append(manifest)
        payloads.extend((path, archive, smoke_path))
        expected_files.update((path, archive, smoke_path))
    if seen != set(TARGETS):
        raise ValueError("candidate set must contain exactly the four expected native targets")
    actual_files = set(inputs.glob("*/*"))
    if actual_files != expected_files or set(p.name for p in inputs.iterdir()) != {"candidate-" + target for target in TARGETS}:
        raise ValueError("unexpected candidate files or directories")
    if any(m["build"]["git_dependencies"] != manifests[0]["build"]["git_dependencies"] for m in manifests):
        raise ValueError("candidate Git dependency provenance differs")
    require_empty(output)
    for path in payloads:
        shutil.copyfile(path, output / path.name)
        if digest(path) != digest(output / path.name):
            raise ValueError("payload copy hash mismatch")
    write_json(output / "provenance.json", {"schema_version": 1, "source": source, "ci": ci,
               "candidates": manifests, "publication": "not authorized or performed"})
    files = sorted(output.iterdir())
    (output / "SHA256SUMS").write_text("".join(f"{digest(path)}  {path.name}\n" for path in files), encoding="ascii")


if __name__ == "__main__":
    main()
