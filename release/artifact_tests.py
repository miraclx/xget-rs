"""Validator tests use synthetic binaries; they are not native executable smoke evidence."""

import copy
import fcntl
import importlib.util
import io
import json
import os
from pathlib import Path
import tarfile
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("artifact", Path(__file__).with_name("artifact.py"))
artifact = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(artifact)

SOURCE = {"sha": "a" * 40, "tree": "b" * 40, "lock_sha256": "c" * 64, "version": "0.2.0-rc.1",
          "repository": "owner/repo", "workflow_run_id": "123", "workflow_attempt": "1", "workflow_ref": "owner/repo/.github/workflows/release.yml@refs/heads/main"}


def build_fixture(target):
    lock = artifact.tomllib.loads((artifact.ROOT / "Cargo.lock").read_text())
    deps = [{"name": p["name"], "version": p["version"], "source": p["source"]}
            for p in lock["package"] if p.get("source", "").startswith("git+")]
    return {"schema_version": 1, "source": copy.deepcopy(SOURCE), "target": target,
            "features": list(artifact.FEATURES), "runner": artifact.TARGETS[target],
            "runner_image": {"ImageOS": "test-image", "ImageVersion": "test-version"},
            "build_command": artifact.build_command(target),
            "tools": {"rustc": "rustc 1.97.1 (fixture)\nhost: " + target, "cargo": "cargo 1.97.1 (fixture)"},
            "binary_sha256": artifact.hashlib.sha256(b"synthetic binary, never executed").hexdigest(),
            "binary_inspection": {"file": "synthetic fixture"}, "git_dependencies": deps, "strip": "not performed"}


def write_candidate(directory, target):
    directory.mkdir()
    build = build_fixture(target)
    stem = f"xget-{SOURCE['version']}-{target}"
    archive = directory / (stem + ".tar.gz")
    data = {"xget": b"synthetic binary, never executed", "build.json": json.dumps(build).encode(),
            "INSTALL.txt": (artifact.ROOT / "release/INSTALL.txt").read_bytes(),
            "LICENSE-MIT": (artifact.ROOT / "LICENSE-MIT").read_bytes(),
            "LICENSE-APACHE": (artifact.ROOT / "LICENSE-APACHE").read_bytes()}
    with tarfile.open(archive, "w:gz") as bundle:
        for name, contents in data.items():
            member = tarfile.TarInfo(name)
            member.size = len(contents)
            bundle.addfile(member, io.BytesIO(contents))
    smoke = directory / (stem + ".smoke.json")
    receipt = {"schema_version": 1, "status": "passed", "target": target, "version": SOURCE["version"],
               "binary_sha256": build["binary_sha256"], "archive_sha256": artifact.digest(archive),
               "cases": {case: {"status": "passed"} for case in artifact.CASES}}
    receipt["cases"]["range_restoration"]["restored_bytes"] = 65536
    artifact.write_json(smoke, receipt)
    manifest = {"schema_version": 1, "build": build, "archive": artifact.file_record(archive), "smoke": artifact.file_record(smoke)}
    path = directory / (stem + ".manifest.json")
    artifact.write_json(path, manifest)
    return path, manifest


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.inputs = self.root / "inputs"
        self.inputs.mkdir()
        self.paths = {}
        for target in artifact.TARGETS:
            self.paths[target] = write_candidate(self.inputs / ("candidate-" + target), target)
        self.ci = self.root / "ci.json"
        artifact.write_json(self.ci, {"head_sha": SOURCE["sha"], "repository": SOURCE["repository"],
            "conclusion": "success", "event": "push", "path": ".github/workflows/ci.yml",
            "jobs": {name: "success" for name in ("rustfmt (nightly)", "clippy", "test", "windows check")}})
        self.source_patch = patch.object(artifact, "source_identity", return_value=SOURCE)
        self.source_patch.start()
        self.env_patch = patch.dict(os.environ, {"RUNNER_TEMP": str(self.root)})
        self.env_patch.start()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.source_patch.stop)
        self.addCleanup(self.env_patch.stop)

    def aggregate(self):
        artifact.aggregate_candidates(self.inputs, self.ci, self.root / "output")

    def first(self):
        return next(iter(self.paths.values()))

    def test_exact_four_targets_produce_non_circular_complete_checksums(self):
        self.aggregate()
        output = self.root / "output"
        sums = (output / "SHA256SUMS").read_text().splitlines()
        self.assertEqual(len(sums), 13)
        self.assertEqual(len(artifact.read_json(output / "provenance.json")["candidates"]), 4)
        for line in sums:
            checksum, name = line.split("  ")
            self.assertNotEqual(name, "SHA256SUMS")
            self.assertEqual(artifact.digest(output / name), checksum)
        self.assertTrue(any(line.endswith("  provenance.json") for line in sums))

    def test_missing_manifest_is_not_an_empty_success(self):
        self.first()[0].unlink()
        with self.assertRaises(ValueError):
            self.aggregate()

    def test_missing_payload_is_rejected(self):
        path, manifest = self.first()
        (path.parent / manifest["archive"]["name"]).unlink()
        with self.assertRaises(ValueError):
            self.aggregate()

    def test_corrupt_archive_is_rejected_before_extraction(self):
        path, manifest = self.first()
        with (path.parent / manifest["archive"]["name"]).open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaises(ValueError):
            self.aggregate()

    def test_duplicate_target_is_rejected(self):
        write_candidate(self.inputs / "candidate-extra", next(iter(artifact.TARGETS)))
        with self.assertRaises(ValueError):
            self.aggregate()

    def test_unexpected_payload_is_rejected(self):
        (self.first()[0].parent / "unexpected.txt").write_text("unexpected")
        with self.assertRaises(ValueError):
            self.aggregate()

    def test_malformed_manifest_is_rejected(self):
        self.first()[0].write_text('{"schema_version":')
        with self.assertRaises(ValueError):
            self.aggregate()

    def test_duplicate_json_keys_are_rejected(self):
        self.first()[0].write_text('{"schema_version":1,"schema_version":1}')
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            self.aggregate()

    def test_missing_fields_are_rejected(self):
        path, manifest = self.first()
        del manifest["build"]["tools"]
        artifact.write_json(path, manifest)
        with self.assertRaises((ValueError, KeyError)):
            self.aggregate()

    def test_mismatched_provenance_and_policy_are_rejected(self):
        _, original = self.first()
        variants = []
        for key, value in (("sha", "d" * 40), ("tree", "d" * 40), ("lock_sha256", "d" * 64),
                           ("version", "0.2.0-rc.2"), ("workflow_attempt", "2")):
            variant = copy.deepcopy(original)
            variant["build"]["source"][key] = value
            variants.append(variant)
        for key, value in (("features", ["s3"]), ("target", "unknown"), ("runner", "ubuntu-latest"),
                           ("binary_sha256", "bad"), ("build_command", ["cargo", "build"]), ("git_dependencies", [])):
            variant = copy.deepcopy(original)
            variant["build"][key] = value
            variants.append(variant)
        for manifest in variants:
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                artifact.validate_manifest(manifest, SOURCE)

    def test_failed_missing_or_unanchored_smoke_is_rejected(self):
        path, manifest = self.first()
        original = artifact.read_json(path.parent / manifest["smoke"]["name"])
        variants = []
        for key, value in (("status", "failed"), ("archive_sha256", "0" * 64), ("binary_sha256", "0" * 64), ("cases", {})):
            receipt = copy.deepcopy(original)
            receipt[key] = value
            variants.append(receipt)
        receipt = copy.deepcopy(original)
        receipt["cases"]["checked_http"]["status"] = "failed"
        variants.append(receipt)
        receipt = copy.deepcopy(original)
        receipt["cases"]["range_restoration"]["restored_bytes"] = 0
        variants.append(receipt)
        for receipt in variants:
            with self.subTest(receipt=receipt), self.assertRaises(ValueError):
                artifact.validate_smoke(receipt, manifest)

    def test_prior_source_ci_does_not_accept_candidate(self):
        ci = artifact.read_json(self.ci)
        ci["head_sha"] = "d" * 40
        artifact.write_json(self.ci, ci)
        with self.assertRaisesRegex(ValueError, "exact-source CI"):
            self.aggregate()

    def test_skipped_ci_job_does_not_accept_candidate(self):
        ci = artifact.read_json(self.ci)
        ci["jobs"]["test"] = "skipped"
        artifact.write_json(self.ci, ci)
        with self.assertRaisesRegex(ValueError, "exact-source CI"):
            self.aggregate()

    def test_internal_binary_and_metadata_must_match_outer_manifest(self):
        path, manifest = self.first()
        manifest["build"]["binary_sha256"] = "0" * 64
        receipt_path = path.parent / manifest["smoke"]["name"]
        receipt = artifact.read_json(receipt_path)
        receipt["binary_sha256"] = "0" * 64
        artifact.write_json(receipt_path, receipt)
        manifest["smoke"] = artifact.file_record(receipt_path)
        artifact.write_json(path, manifest)
        with self.assertRaisesRegex(ValueError, "internal metadata or binary"):
            self.aggregate()


class ResourceBoundaryTests(unittest.TestCase):
    def test_actual_oversized_or_mismatched_payload_is_not_hashed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payload = root / "payload.tar.gz"
            payload.write_bytes(b"A" * 65537)
            record = {"name": payload.name, "size": 1, "sha256": "0" * 64}
            with patch.object(artifact, "MAX_ARCHIVE", 65536), patch.object(artifact, "digest") as digest:
                with self.assertRaises(ValueError):
                    artifact.checked_file(root, record)
                digest.assert_not_called()
            payload.write_bytes(b"small but not the claimed size")
            with patch.object(artifact, "digest") as digest:
                with self.assertRaises(ValueError):
                    artifact.checked_file(root, record)
                digest.assert_not_called()

    def test_outer_timeout_terminates_stalled_owned_descendant_and_records_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lock_path, ready = root / "descendant.lock", root / "ready"
            child = ("import fcntl,os,signal,time; from pathlib import Path; "
                     "signal.signal(signal.SIGINT,signal.SIG_IGN); "
                     f"stream=open({str(lock_path)!r},'w'); fcntl.flock(stream,fcntl.LOCK_EX); "
                     f"Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(10)")
            parent = ("import signal,subprocess,sys,time; signal.signal(signal.SIGINT,signal.SIG_IGN); "
                      f"subprocess.Popen([sys.executable,'-I','-B','-c',{child!r}]); time.sleep(10)")
            receipt = root / "timeout.json"
            with self.assertRaises(artifact.subprocess.TimeoutExpired):
                artifact.run_smoke_process([sys.executable, "-I", "-B", "-c", parent], receipt, timeout=1, grace=0.1)
            self.assertTrue(ready.is_file(), "owned descendant must actually have started")
            with lock_path.open("a") as stream:
                deadline = artifact.time.monotonic() + 2
                while True:
                    try:
                        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        self.assertLess(artifact.time.monotonic(), deadline, "owned descendant still holds its lock")
                        artifact.time.sleep(0.01)
            facts = artifact.read_json(receipt)
            self.assertEqual(facts["status"], "failed")
            self.assertTrue(facts["forced_group_signal_sent"])
            self.assertEqual(facts["leader_exit_code"], -artifact.signal.SIGKILL)


class ExtractionTests(unittest.TestCase):
    def test_hidden_pax_and_gnu_extensions_are_refused_before_tarfile_parser(self):
        for kind in (tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archive = root / "extension.tar.gz"
                header = tarfile.TarInfo("metadata")
                header.type = kind
                header.size = 256 * 1024
                with artifact.gzip.open(archive, "wb") as compressed:
                    compressed.write(header.tobuf(format=tarfile.USTAR_FORMAT))
                    compressed.write(b"A" * header.size)
                    compressed.write(bytes(1024))
                self.assertLess(archive.stat().st_size, 65536)
                destination = root / "empty"
                destination.mkdir()
                with patch.object(artifact, "MAX_ARCHIVE", 65536), patch.object(artifact.tarfile, "open") as parser:
                    with self.assertRaisesRegex(ValueError, "extensions are refused"):
                        artifact.extract_archive(archive, destination)
                    parser.assert_not_called()
                self.assertEqual(list(destination.iterdir()), [])

    def test_expanded_header_and_trailing_padding_bytes_count_before_parser(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "padding.tar.gz"
            with tarfile.open(archive, "w:gz", format=tarfile.USTAR_FORMAT) as bundle:
                for name in sorted(artifact.MEMBERS):
                    bundle.addfile(tarfile.TarInfo(name))
            self.assertLess(archive.stat().st_size, 4096)
            destination = root / "empty"
            destination.mkdir()
            with patch.object(artifact, "MAX_ARCHIVE", 4096), patch.object(artifact.tarfile, "open") as parser:
                with self.assertRaisesRegex(ValueError, "including headers and padding"):
                    artifact.extract_archive(archive, destination)
                parser.assert_not_called()
            self.assertEqual(list(destination.iterdir()), [])

    def test_trailing_nonzero_or_truncated_body_is_refused_without_writes(self):
        for data in (b"truncated", bytes(1024) + b"hidden second archive"):
            with self.subTest(data=data), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archive = root / "bad.tar.gz"
                with artifact.gzip.open(archive, "wb") as compressed:
                    compressed.write(data)
                destination = root / "empty"
                destination.mkdir()
                with self.assertRaises(ValueError):
                    artifact.extract_archive(archive, destination)
                self.assertEqual(list(destination.iterdir()), [])

    def test_unsafe_members_are_rejected_without_writing_any_file(self):
        for name, kind in (("../escape", tarfile.REGTYPE), ("/escape", tarfile.REGTYPE),
                           ("xget", tarfile.SYMTYPE), ("xget", tarfile.LNKTYPE),
                           ("xget", tarfile.FIFOTYPE), ("nested/xget", tarfile.REGTYPE)):
            with self.subTest(name=name, kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archive = root / "unsafe.tar.gz"
                with tarfile.open(archive, "w:gz") as bundle:
                    member = tarfile.TarInfo(name)
                    member.type = kind
                    member.linkname = "../escape"
                    bundle.addfile(member)
                destination = root / "empty"
                destination.mkdir()
                with self.assertRaises(ValueError):
                    artifact.extract_archive(archive, destination)
                self.assertEqual(list(destination.iterdir()), [])
                self.assertFalse((root / "escape").exists())

    def test_missing_duplicate_and_oversized_members_are_rejected(self):
        for names in ([], ["xget"], ["xget", "xget"]):
            with self.subTest(names=names), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archive = root / "invalid.tar.gz"
                with tarfile.open(archive, "w:gz") as bundle:
                    for name in names:
                        bundle.addfile(tarfile.TarInfo(name))
                destination = root / "empty"
                destination.mkdir()
                with self.assertRaises(ValueError):
                    artifact.extract_archive(archive, destination)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, manifest = write_candidate(root / "candidate", next(iter(artifact.TARGETS)))
            destination = root / "empty"
            destination.mkdir()
            with patch.object(artifact, "MAX_ARCHIVE", 1), self.assertRaises(ValueError):
                artifact.extract_archive(path.parent / manifest["archive"]["name"], destination)

    def test_nonempty_and_linked_destinations_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, manifest = write_candidate(root / "candidate", next(iter(artifact.TARGETS)))
            destination = root / "used"
            destination.mkdir()
            (destination / "incumbent").write_text("keep")
            with self.assertRaises(ValueError):
                artifact.extract_archive(path.parent / manifest["archive"]["name"], destination)
            link = root / "link"
            link.symlink_to(destination, target_is_directory=True)
            with self.assertRaises(ValueError):
                artifact.extract_archive(path.parent / manifest["archive"]["name"], link)
            self.assertEqual((destination / "incumbent").read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
