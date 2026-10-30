"""Static policy assertions, not a substitute for GitHub's workflow parser or a native run."""

import ast
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]


class WorkflowPolicyTests(unittest.TestCase):
    def test_python_helpers_parse_without_writing_bytecode(self):
        files = sorted((ROOT / "scripts/release").glob("*.py"))
        self.assertGreaterEqual(len(files), 6)
        for path in files:
            with self.subTest(path=path.name):
                ast.parse(path.read_text(), filename=str(path))

    def test_candidate_workflow_cannot_publish_or_trigger_on_tags(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        for forbidden in ("contents: write", "actions: write", "id-token: write", "action-gh-release",
                          "tags:", "cargo publish", "gh release", "git push", "pull_request_target"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, workflow)
        for required in ("contents: read", "actions: read", "branches: [main]", "workflow_dispatch:",
                         "github.event.repository.default_branch", "persist-credentials: false",
                         "needs: build", "scripts/release/ci_gate.py", "--deadline-seconds 5400",
                         "merge-multiple: false", "if-no-files-found: error", "CARGO_INCREMENTAL: '0'",
                         "CARGO_BUILD_JOBS: '2'", "max-parallel: 2"):
            with self.subTest(required=required):
                self.assertIn(required, workflow)
        self.assertEqual(workflow.count("- {target:"), 4)
        for row in ("x86_64-unknown-linux-gnu, runner: ubuntu-24.04}",
                    "aarch64-unknown-linux-gnu, runner: ubuntu-24.04-arm}",
                    "x86_64-apple-darwin, runner: macos-15-intel}",
                    "aarch64-apple-darwin, runner: macos-15}"):
            self.assertIn(row, workflow)

    def test_cargo_paths_are_exported_by_step_only_after_toolchain_setup(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        build = workflow.split("\n  build:\n", 1)[1].split("\n  accept:\n", 1)[0]
        job_header, steps = build.split("\n    steps:\n", 1)
        # runner context is valid in steps, but not in a job-level env expression.
        self.assertNotIn("runner.temp", job_header)
        self.assertNotIn("CARGO_HOME", job_header)
        self.assertNotIn("CARGO_TARGET_DIR", job_header)
        self.assertIn("CANDIDATE_RUNNER: ${{ matrix.runner }}", job_header)
        setup = steps.index("- uses: dtolnay/rust-toolchain@1.97.1")
        isolation = steps.index("- name: Isolate candidate Cargo directories after toolchain setup")
        build_step = steps.index("- name: Build, package and smoke extracted final bytes")
        self.assertLess(setup, isolation)
        self.assertLess(isolation, build_step)
        export_step = steps[isolation:].split("\n      - ", 1)[0]
        self.assertIn("shell: bash", export_step)
        for variable, suffix in (("CARGO_HOME", "candidate-cargo"), ("CARGO_TARGET_DIR", "candidate-target")):
            command = "printf '" + variable + "=%s/" + suffix + "\\n' \"$RUNNER_TEMP\" >> \"$GITHUB_ENV\""
            self.assertIn(command, export_step)
        self.assertNotIn("${{ runner.temp }}", export_step)

    def test_owned_prose_and_code_have_no_em_dashes(self):
        for path in (ROOT / "scripts/release").iterdir():
            if path.is_file():
                with self.subTest(path=path.name):
                    self.assertNotIn(chr(0x2014), path.read_text())
        self.assertNotIn(chr(0x2014), (ROOT / ".github/workflows/release.yml").read_text())


if __name__ == "__main__":
    unittest.main()
