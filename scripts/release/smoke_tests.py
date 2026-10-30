"""Check the fixture and receipt predicates, without executing or certifying an xget build."""

import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

SPEC = importlib.util.spec_from_file_location("smoke", Path(__file__).with_name("smoke.py"))
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


class ReceiptTests(unittest.TestCase):
    def test_empty_or_malformed_receipts_cannot_pass(self):
        for data in (b"", b"not json", b"{}", b'{"schema_version":1,"kind":"end"}'):
            with self.subTest(data=data), self.assertRaises((ValueError, KeyError)):
                smoke.events(data)

    def test_elapsed_units_and_required_event_count_are_checked(self):
        record = {"schema_version": 1, "kind": "End", "elapsed_ms": 10, "facts": {"complete": True}}
        records = smoke.events(json.dumps(record).encode())
        self.assertEqual(smoke.one_event(records, "End"), {"complete": True})
        for events in ([], records * 2):
            with self.assertRaises(ValueError):
                smoke.one_event(events, "End")
        record["elapsed_ms"] = 0.01
        with self.assertRaises(ValueError):
            smoke.events(json.dumps(record).encode())


class CaptureTests(unittest.TestCase):
    def child(self, code, stderr=smoke.subprocess.PIPE):
        return smoke.subprocess.Popen([sys.executable, "-I", "-B", "-c", code],
                                      stdout=smoke.subprocess.PIPE, stderr=stderr)

    def test_both_channels_are_drained_and_returned_exactly(self):
        process = self.child("import os; os.write(1,b'payload'); os.write(2,b'diagnostics')")
        with smoke.Capture(process) as capture:
            result = capture.finish(timeout=5)
        self.assertEqual(result.stdout, b"payload")
        self.assertEqual(result.stderr, b"diagnostics")
        self.assertTrue(all(not reader.is_alive() for reader in capture.readers))

    def test_shared_budget_rejects_two_individually_small_channels(self):
        process = self.child("import os,time; os.write(1,b'x'*40); os.write(2,b'y'*40); time.sleep(10)")
        with patch.object(smoke, "MAX_LOG", 64), smoke.Capture(process) as capture:
            with self.assertRaisesRegex(ValueError, "output exceeds receipt bound"):
                capture.finish(timeout=5)
        self.assertEqual(sum(map(len, capture.output)), 64)
        self.assertTrue(all(capture.output))
        self.assertLess(process.returncode, 0)
        self.assertTrue(all(not reader.is_alive() for reader in capture.readers))

    def test_finite_newline_free_output_on_either_channel_is_bounded_during_receive(self):
        for channel in (1, 2):
            with self.subTest(channel=channel):
                process = self.child(f"import os,time; os.write({channel},b'x'*4096); time.sleep(10)")
                with patch.object(smoke, "MAX_LOG", 64), smoke.Capture(process) as capture:
                    with self.assertRaisesRegex(ValueError, "output exceeds receipt bound"):
                        capture.finish(timeout=5)
                self.assertEqual(sum(map(len, capture.output)), 64)
                self.assertLess(process.returncode, 0)
                self.assertTrue(all(not reader.is_alive() for reader in capture.readers))

    def test_tty_capture_also_bounds_stdout_and_joins_both_drains(self):
        master, slave = smoke.pty.openpty()
        try:
            process = self.child("import os,time; os.write(1,b'x'*4096); time.sleep(10)", stderr=slave)
            os.close(slave)
            slave = None
            with patch.object(smoke, "MAX_LOG", 64), smoke.Capture(process, stderr_fd=master) as capture:
                with self.assertRaisesRegex(ValueError, "output exceeds receipt bound"):
                    capture.finish(timeout=5)
            self.assertLessEqual(sum(map(len, capture.output)), 64)
            self.assertLess(process.returncode, 0)
            self.assertTrue(all(not reader.is_alive() for reader in capture.readers))
        finally:
            os.close(master)
            if slave is not None:
                os.close(slave)


class OwnedDataTests(unittest.TestCase):
    def test_saved_state_accepts_canonical_alias_of_owned_root(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory).resolve()
            root = parent / "real"
            root.mkdir()
            alias = parent / "alias"
            alias.symlink_to(root, target_is_directory=True)
            state = root / "saved"
            state.mkdir()
            self.assertFalse(state.is_relative_to(alias))
            self.assertEqual(smoke.owned_state(alias, str(state)), state)
            self.assertEqual(smoke.owned_state(root, str(alias / "saved")), state)

    def test_external_or_link_escaped_state_is_still_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory).resolve()
            root = parent / "owned"
            root.mkdir()
            foreign = parent / "foreign"
            foreign.mkdir()
            (root / "escape").symlink_to(foreign, target_is_directory=True)
            for state in (foreign, root / "escape"):
                with self.subTest(state=state), self.assertRaisesRegex(ValueError, "outside owned data"):
                    smoke.owned_state(root, str(state))

    def test_missing_or_nondirectory_state_is_not_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "file").write_text("not state")
            with self.assertRaises(FileNotFoundError):
                smoke.owned_state(root, str(root / "missing"))
            with self.assertRaisesRegex(ValueError, "not a directory"):
                smoke.owned_state(root, str(root / "file"))

    def test_payload_cases_get_distinct_new_empty_canonical_directories(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            instance = smoke.Smoke(Path(sys.executable), root, None, {})
            first = instance.data_directory("first")
            second = instance.data_directory("second")
            self.assertEqual(instance.root, root.resolve())
            self.assertEqual(list(first.iterdir()), [])
            self.assertEqual(list(second.iterdir()), [])
            self.assertNotEqual(first, second)
            (first / "payload").write_bytes(b"owned data")
            with self.assertRaises(FileExistsError):
                instance.data_directory("first")
            self.assertEqual(list(second.iterdir()), [])


class FixtureTests(unittest.TestCase):
    def setUp(self):
        self.fixture = smoke.Fixture().__enter__()
        self.addCleanup(self.fixture.__exit__, None, None, None)
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(self, path, method="GET", headers=None):
        return self.opener.open(urllib.request.Request(self.fixture.url(path), method=method, headers=headers or {}), timeout=5)

    def test_owned_exact_payload_and_head_metadata_match(self):
        with self.request("tiny.bin", "HEAD") as response:
            self.assertEqual(int(response.headers["Content-Length"]), len(smoke.TINY))
            self.assertEqual(response.headers["ETag"], smoke.ETAG)
            self.assertEqual(response.headers["Connection"], "close")
            self.assertEqual(response.read(), b"")
        with self.request("tiny.bin") as response:
            self.assertEqual(response.read(), smoke.TINY)
        self.assertEqual(len(self.fixture.requests_snapshot()), 2)

    def test_real_range_response_and_conditional_refusal_are_distinct(self):
        self.fixture.slow_ranges = False
        with self.request("range.bin", headers={"Range": "bytes=17-55", "If-Match": smoke.ETAG}) as response:
            self.assertEqual(response.status, 206)
            self.assertEqual(response.headers["Content-Range"], f"bytes 17-55/{len(smoke.LARGE)}")
            self.assertEqual(response.read(), smoke.LARGE[17:56])
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request("range.bin", headers={"Range": "bytes=17-55", "If-Match": '"foreign"'})
        self.assertEqual(caught.exception.code, 412)
        caught.exception.close()

    def test_disclosed_negative_fixtures_are_real_protocol_failures(self):
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request("denied.bin", "HEAD")
        self.assertEqual(caught.exception.code, 403)
        caught.exception.close()
        with self.request("ignored.bin", headers={"Range": "bytes=17-55", "If-Match": smoke.ETAG}) as response:
            self.assertEqual(response.status, 200)
            self.assertIsNone(response.headers.get("Content-Range"))
            self.assertEqual(response.read(), smoke.LARGE)
        self.fixture.offline = True
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request("tiny.bin")
        self.assertEqual(caught.exception.code, 503)
        caught.exception.close()


if __name__ == "__main__":
    unittest.main()
