"""Exercise only the final extracted executable against owned loopback fixtures."""

import argparse
import hashlib
import http.server
import importlib.util
import json
import os
from pathlib import Path
import platform
import pty
import re
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import fcntl
import termios

# -I deliberately excludes the script directory from import search. Load this trusted sibling by path.
SPEC = importlib.util.spec_from_file_location("candidate_artifact", Path(__file__).with_name("artifact.py"))
artifact = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(artifact)
TINY = b"xget candidate checked HTTP\n"
LARGE = (b"owned candidate range fixture\n" * 150000)[:4 * 1024 * 1024]
ETAG = '"candidate-fixture-v1"'
MAX_LOG = 4 * 1024 * 1024


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--target", choices=artifact.TARGETS, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--binary-sha256", required=True)
    args = parser.parse_args()
    receipt = {"schema_version": 1, "status": "failed", "cases": {}, "target": args.target,
               "version": args.version, "archive_sha256": artifact.digest(args.archive),
               "binary_sha256": args.binary_sha256}
    try:
        with tempfile.TemporaryDirectory(prefix="candidate-smoke-", dir=os.environ["RUNNER_TEMP"]) as directory:
            root = Path(directory)
            extracted = root / "extracted"
            extracted.mkdir()
            artifact.extract_archive(args.archive, extracted)
            binary = extracted / "xget"
            require(artifact.digest(binary) == args.binary_sha256, "extracted binary hash mismatch")
            expected_machine = "aarch64" if args.target.startswith("aarch64") else "x86_64"
            actual_machine = {"arm64": "aarch64", "AMD64": "x86_64"}.get(platform.machine(), platform.machine())
            require(actual_machine == expected_machine, "smoke host architecture mismatch")
            with Fixture() as fixture:
                smoke = Smoke(binary, root, fixture, receipt["cases"])
                smoke.run(args.version)
            require(set(receipt["cases"]) == artifact.CASES, "smoke case set incomplete")
            require(artifact.digest(binary) == args.binary_sha256, "binary changed during smoke")
        receipt["status"] = "passed"
    except Exception as error:
        receipt["failure"] = str(error)
        raise
    finally:
        artifact.write_json(args.receipt, receipt)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def events(data):
    result = [json.loads(line) for line in data.splitlines()]
    require(bool(result), "JSON receipt is empty")
    for event in result:
        require(event["schema_version"] == 1 and re.fullmatch(r"[A-Z][A-Za-z]+", event["kind"]), "invalid JSON envelope")
        require(type(event["elapsed_ms"]) is int and event["elapsed_ms"] >= 0, "elapsed time is not integer milliseconds")
        require(isinstance(event["facts"], dict), "missing structured facts")
    return result


def one_event(records, kind):
    found = [event["facts"] for event in records if event["kind"] == kind]
    require(len(found) == 1, f"expected exactly one {kind} event")
    return found[0]


def owned_state(root, reported):
    canonical_root = root.resolve(strict=True)
    state = Path(reported).resolve(strict=True)
    require(state.is_relative_to(canonical_root) and state.is_dir(),
            f"saved-run path is outside owned data or not a directory: reported={reported!r}, root={str(canonical_root)!r}")
    return state


class Capture:
    """Own concurrent fixed-size drains and one shared retained-output budget for a child."""
    def __init__(self, process, stderr_fd=None):
        self.process = process
        self.output = [bytearray(), bytearray()]
        self.retained = 0
        self.failure = None
        self.failed = threading.Event()
        # Both drain threads debit the same retained byte budget before appending.
        self.lock = threading.Lock()
        self.readers = []
        self.closed = False
        descriptors = (process.stdout.fileno(), process.stderr.fileno() if stderr_fd is None else stderr_fd)
        for channel, descriptor in enumerate(descriptors):
            reader = threading.Thread(target=self.drain, args=(channel, descriptor, stderr_fd is not None and channel == 1), daemon=True)
            self.readers.append(reader)
            reader.start()

    def drain(self, channel, descriptor, terminal):
        try:
            while True:
                try:
                    block = os.read(descriptor, 16384)
                except OSError as error:
                    if terminal and error.errno == 5:
                        return
                    raise
                if not block:
                    return
                with self.lock:
                    accepted = min(len(block), MAX_LOG - self.retained)
                    self.output[channel].extend(block[:accepted])
                    self.retained += accepted
                    overflow = accepted != len(block)
                if overflow:
                    self.fail("command output exceeds receipt bound")
        except Exception as error:
            self.fail(f"cannot capture owned child output: {error}")

    def fail(self, message):
        with self.lock:
            if self.failure is None:
                self.failure = message
        self.failed.set()
        self.kill()

    def kill(self):
        try:
            self.process.kill()
        except ProcessLookupError:
            pass

    def finish(self, timeout=45):
        self.process.wait(timeout=timeout)
        for reader in self.readers:
            reader.join(timeout=5)
        require(not any(reader.is_alive() for reader in self.readers), "owned child output drain did not settle")
        require(not self.failed.is_set(), self.failure)
        return subprocess.CompletedProcess(self.process.args, self.process.returncode, bytes(self.output[0]), bytes(self.output[1]))

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self.process.poll() is None:
                self.kill()
            self.process.wait(timeout=5)
        finally:
            for reader in self.readers:
                reader.join(timeout=5)
            for stream in (self.process.stdout, self.process.stderr):
                if stream is not None:
                    stream.close()
        require(not any(reader.is_alive() for reader in self.readers), "owned child output drain did not settle")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class Smoke:
    def __init__(self, binary, root, fixture, cases):
        self.binary, self.root = binary.resolve(strict=True), root.resolve(strict=True)
        self.fixture, self.cases = fixture, cases
        self.env = dict(os.environ)
        for name in tuple(self.env):
            if name.lower().endswith("_proxy"):
                self.env.pop(name)
        self.env.update({"HOME": str(self.root), "TERM": "xterm-256color", "LC_ALL": "C.UTF-8",
                         "AWS_EC2_METADATA_DISABLED": "true"})
        self.env.pop("NO_COLOR", None)

    def run(self, version):
        version_result = self.command("--version")
        require(version_result.stdout.strip() == f"xget {version}".encode(), "incorrect binary version")
        help_results = [self.command(*args).stdout.decode() for args in
                        (("--help",), ("list", "--help"), ("info", "--help"), ("resume", "--help"))]
        require(all("Usage:" in text for text in help_results), "missing command help")
        require("--expect" in help_results[0] and "--input" in help_results[3], "missing checked-fetch or binding help")
        self.pass_case("help_version", version=version_result.stdout.decode().strip(), host=platform.platform())

        path = self.data_directory("checked-http") / "human.bin"
        expected = hashlib.sha256(TINY).hexdigest()
        result = self.command(self.fixture.url("tiny.bin"), "-o", str(path), "--expect", "sha256:" + expected, "--progress", "none", cwd=path.parent)
        require(path.read_bytes() == TINY and result.stdout == b"", "checked file or output channel mismatch")
        require(b"saved" in result.stderr and expected.encode() in result.stderr, "missing human checked success receipt")
        require(b"\x1b" not in result.stderr and b"\r" not in result.stderr, "captured human output contains terminal styling")
        self.pass_case("checked_http", sha256=expected, bytes=len(TINY), receipt=result.stderr.decode())

        wrong = self.data_directory("required-mismatch") / "mismatch.bin"
        result = self.command(self.fixture.url("tiny.bin"), "-o", str(wrong), "--expect", "sha256:" + "0" * 64, "--json", outcome="failure", cwd=wrong.parent)
        records = events(result.stderr)
        require(not wrong.exists() and not result.stdout, "mismatched bytes were published or leaked to stdout")
        require({"RunFailed", "Error"} <= {event["kind"] for event in records}, "failure events missing")
        self.pass_case("required_mismatch", exit_code=result.returncode, events=records)

        incumbent = self.data_directory("incumbent") / "incumbent.bin"
        incumbent.write_bytes(b"owned incumbent\n")
        before = incumbent.stat()
        self.command(self.fixture.url("tiny.bin"), "-o", str(incumbent), "--progress", "none", outcome="failure", cwd=incumbent.parent)
        require(incumbent.read_bytes() == b"owned incumbent\n" and incumbent.stat().st_ino == before.st_ino, "incumbent was replaced")
        self.pass_case("incumbent_preserved", sha256=artifact.digest(incumbent))

        path = self.data_directory("json-units") / "json.bin"
        large_expected = hashlib.sha256(LARGE).hexdigest()
        result = self.command(self.fixture.url("slow.bin"), "-o", str(path), "--expect", "sha256:" + large_expected, "--json", "--bits", "--raw-sizes", cwd=path.parent)
        records = events(result.stderr)
        published = one_event(records, "Published")
        require(result.stdout == b"" and path.read_bytes() == LARGE, "JSON payload isolation failed")
        require(published["length_bytes"] == len(LARGE) and published["logical_written_bytes"] == len(LARGE), "JSON byte units changed")
        require(published["checks"]["measured_digest"] == "sha256:" + large_expected, "measurement missing")
        require(any(check["digest"] == "sha256:" + large_expected for check in published["checks"]["matched_requirements"]), "required check receipt missing")
        require(one_event(records, "End")["completed_files"] == 1, "JSON completion missing")
        progress = [event["facts"] for event in records if event["kind"] == "Progress"]
        require(bool(progress), "JSON rate/units case observed no progress samples")
        for facts in progress:
            rate = facts["rate_bytes_per_second"]
            require(type(rate) is int and rate >= 0, "JSON rate is not bytes per second")
        require(any(facts["rate_bytes_per_second"] > 0 for facts in progress), "JSON case observed no positive source rate")
        self.pass_case("json_units", events=records)

        result = self.command(self.fixture.url("tiny.bin"), "-o", "-", "--expect", "sha256:" + expected, "--json", cwd=self.data_directory("payload-isolation"))
        require(result.stdout == TINY and one_event(events(result.stderr), "End")["published_files"] == 0, "live payload or publication facts incorrect")
        self.pass_case("payload_isolation", payload_sha256=hashlib.sha256(result.stdout).hexdigest(), stderr_events=events(result.stderr))
        self.tty()
        self.restore()
        self.negatives()

    def data_directory(self, name):
        directory = self.root / name
        directory.mkdir()
        return directory

    def command(self, *args, outcome="success", cwd=None):
        process = subprocess.Popen([str(self.binary), *args], cwd=cwd or self.root, env=self.env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with Capture(process) as capture:
            result = capture.finish()
        require((result.returncode == 0) if outcome == "success" else (result.returncode > 0),
                f"unexpected exit {result.returncode}: {args}: {result.stderr[:2000]!r}")
        return result

    def pass_case(self, name, **facts):
        self.cases[name] = {"status": "passed", **facts}

    def tty(self):
        destination = self.data_directory("native-tty") / "tty.bin"
        master, slave = pty.openpty()
        process = None
        try:
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 100, 0, 0))
            process = subprocess.Popen([str(self.binary), self.fixture.url("slow.bin"), "-o", str(destination), "--progress", "bar"],
                                       cwd=destination.parent, env=self.env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=slave)
            os.close(slave)
            slave = None
            with Capture(process, stderr_fd=master) as capture:
                result = capture.finish()
            output = result.stderr
            require(result.returncode == 0 and result.stdout == b"" and destination.read_bytes() == LARGE, "native TTY download failed")
            require(b"saved" in output and (b"\x1b[" in output or b"\r" in output), "native terminal rendering was not observed")
            self.pass_case("native_tty", columns=100, stderr=output.decode(errors="replace"), sha256=artifact.digest(destination))
        finally:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            os.close(master)
            if slave is not None:
                os.close(slave)

    def restore(self):
        work = self.data_directory("range-work")
        destination = work / "restored.bin"
        url = self.fixture.url("range.bin")
        process = subprocess.Popen([str(self.binary), url, "-o", str(destination), "--json"],
                                   cwd=work, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        capture = Capture(process)
        try:
            require(self.fixture.range_tails_done.wait(15) and not capture.failed.is_set(), "initial range tails did not settle")
            deadline = time.monotonic() + 15
            native = []
            while time.monotonic() < deadline and process.poll() is None:
                native = list(work.glob(".xget-native-*/state"))
                if len(native) == 1:
                    break
                time.sleep(0.01)
            require(len(native) == 1, "expected one real native checkpoint")
            require(native[0].stat().st_size <= 128 * 1024, "native fixture state exceeds bound")
            before = native[0].read_bytes()
            self.fixture.range_head_release.set()
            # The response remains held, so completion cannot race the checkpoint observation.
            while time.monotonic() < deadline and native[0].read_bytes() == before:
                require(process.poll() is None and not capture.failed.is_set(), "range transfer ended before checkpoint")
                time.sleep(0.01)
            require(native[0].read_bytes() != before, "no acknowledged contiguous prefix checkpoint")
            require(process.poll() is None, "range transfer completed before controlled interruption")
            process.send_signal(signal.SIGINT)
            result = capture.finish(timeout=20)
            require(result.returncode == 130 and not destination.exists(), "interruption falsely completed or published")
            require(result.stdout == b"", "interruption payload leaked to stdout")
        finally:
            self.fixture.range_head_release.set()
            self.fixture.range_body_release.set()
            capture.close()
        records = events(result.stderr)
        reported = one_event(records, "RunStarted")["state_path"]
        self.cases["interruption"] = {"status": "failed", "exit_code": process.returncode,
                                      "reported_state_path": reported, "data_root": str(self.root), "events": records}
        state = owned_state(self.root, reported)
        self.pass_case("interruption", exit_code=130, events=records, state_path=str(state))

        self.fixture.offline = True
        before = self.fixture.requests_snapshot()
        result = self.command("info", str(state), "--json", cwd=work)
        info = events(result.stdout)
        end = one_event(info, "End")
        require(not result.stderr and end["offline"] is True and end["content_revalidated"] is False and end["execution_authority"] is False,
                "offline info facts incorrect")
        require(self.fixture.requests_snapshot() == before, "offline info made a network request")
        member = one_event(info, "SavedMember")
        self.pass_case("offline_info", events=info, network_requests=0)

        self.fixture.offline = False
        self.fixture.slow_ranges = False
        before = len(self.fixture.requests_snapshot())
        result = self.command("resume", str(state), "--input", member["binding_key"] + "=" + url, "--json", cwd=work)
        resumed = events(result.stderr)
        facts = one_event(resumed, "Published")
        stats = facts["range_attempt"]
        require(stats is not None and 0 < stats["restored_bytes"] < len(LARGE), "no strong recorded prefix restored")
        require(stats["restored_bytes"] + stats["source_bytes"] == len(LARGE), "restored/source byte accounting mismatch")
        require(stats["source_bytes"] == stats["written_bytes"], "current physical writes disagree with source bytes")
        require(destination.read_bytes() == LARGE and not result.stdout, "restored published bytes incorrect")
        requests = self.fixture.requests_snapshot()[before:]
        require(bool(requests) and all(row[0] == "GET" and row[1] == "/range.bin" and row[2] is not None for row in requests), "resume rediscovered or used whole fallback")
        starts = [int(re.fullmatch(r"bytes=(\d+)-(\d+)", row[2]).group(1)) for row in requests]
        require(min(starts) == stats["restored_bytes"], "resumed requests did not begin at the restored prefix")
        self.pass_case("range_restoration", **stats, sha256=artifact.digest(destination), requests=requests, events=resumed)

    def negatives(self):
        for name, path in (("redirect_refused", "redirect.bin"), ("head_denied", "denied.bin"), ("ignored_range_refused", "ignored.bin")):
            destination = self.data_directory(name) / path
            before = len(self.fixture.requests_snapshot())
            result = self.command(self.fixture.url(path), "-o", str(destination), "--json", outcome="failure", cwd=destination.parent)
            records = events(result.stderr)
            require(not destination.exists() and not result.stdout and any(event["kind"] == "Error" for event in records), "unsupported HTTP case did not fail safely")
            requests = self.fixture.requests_snapshot()[before:]
            require(bool(requests), "negative fixture was not exercised")
            if name in {"redirect_refused", "head_denied"}:
                require(all(row[0] == "HEAD" and row[1] == "/" + path for row in requests), "unsupported HEAD unexpectedly followed or fell back")
            else:
                require(any(row[0] == "GET" and row[2] for row in requests), "ignored range case sent no range request")
            self.pass_case(name, exit_code=result.returncode, requests=requests, events=records)


class Fixture:
    def __init__(self):
        self.requests = []
        # HTTP worker threads append while the controlling smoke snapshots request evidence.
        self.lock = threading.Lock()
        self.offline = False
        self.slow_ranges = True
        self.range_head_release = threading.Event()
        self.range_body_release = threading.Event()
        self.range_tails_done = threading.Event()
        self.range_tails = 0
        self.stop = threading.Event()
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def do_HEAD(self):
                self.respond("HEAD")

            def do_GET(self):
                self.respond("GET")

            def respond(self, method):
                with fixture.lock:
                    fixture.requests.append((method, self.path, self.headers.get("Range")))
                if fixture.offline:
                    self.send_error(503)
                    return
                if self.path == "/redirect.bin":
                    self.send_response(302)
                    self.send_header("Location", fixture.url("tiny.bin"))
                    self.send_header("Content-Length", "0")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    return
                if self.path == "/denied.bin" and method == "HEAD":
                    self.send_error(403)
                    return
                if self.path not in {"/tiny.bin", "/slow.bin", "/range.bin", "/ignored.bin", "/denied.bin"}:
                    self.send_error(404)
                    return
                data = LARGE if self.path in {"/range.bin", "/slow.bin", "/ignored.bin"} else TINY
                start, end, status = 0, len(data) - 1, 200
                requested = self.headers.get("Range")
                if method == "GET" and requested and self.path != "/ignored.bin":
                    match = re.fullmatch(r"bytes=(\d+)-(\d+)", requested)
                    if not match or self.headers.get("If-Match") != ETAG:
                        self.send_error(412)
                        return
                    start, end = map(int, match.groups())
                    if not 0 <= start <= end < len(data):
                        self.send_error(416)
                        return
                    status = 206
                self.send_response(status)
                self.send_header("Content-Length", str(end - start + 1))
                self.send_header("Connection", "close")
                self.send_header("ETag", ETAG)
                if self.path in {"/range.bin", "/ignored.bin"}:
                    self.send_header("Accept-Ranges", "bytes")
                if status == 206:
                    self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
                self.end_headers()
                if method == "HEAD":
                    return
                try:
                    initial_range = self.path == "/range.bin" and fixture.slow_ranges
                    if initial_range and start == 0:
                        if not fixture.range_head_release.wait(30):
                            return
                        self.wfile.write(data[:(end + 1) // 2])
                        self.wfile.flush()
                        fixture.range_body_release.wait(30)
                        self.close_connection = True
                        return
                    for offset in range(start, end + 1, 16384):
                        if fixture.stop.is_set():
                            return
                        self.wfile.write(data[offset:min(offset + 16384, end + 1)])
                        self.wfile.flush()
                        if self.path == "/slow.bin":
                            fixture.stop.wait(0.025)
                    if initial_range:
                        with fixture.lock:
                            fixture.range_tails += 1
                            if fixture.range_tails == 3:
                                fixture.range_tails_done.set()
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def url(self, path):
        return f"http://127.0.0.1:{self.server.server_port}/{path}"

    def requests_snapshot(self):
        with self.lock:
            return list(self.requests)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.stop.set()
        self.range_head_release.set()
        self.range_body_release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


if __name__ == "__main__":
    main()
