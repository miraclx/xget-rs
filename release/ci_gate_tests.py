import copy
from email.message import Message
import importlib.util
import io
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.response

SPEC = importlib.util.spec_from_file_location("ci_gate", Path(__file__).with_name("ci_gate.py"))
ci_gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci_gate)
SHA = "a" * 40
RUN = {"id": 123, "run_attempt": 1, "head_sha": SHA, "event": "push", "head_branch": "main",
       "path": ".github/workflows/ci.yml", "status": "completed", "conclusion": "success", "html_url": "https://example.invalid/run/123"}


class CiGateTests(unittest.TestCase):
    def listing(self, runs):
        return {"total_count": len(runs), "workflow_runs": runs}

    def test_actual_request_policy_refuses_all_redirect_destinations_without_forwarding_fake_token(self):
        original_builder = ci_gate.urllib.request.build_opener
        initial = "https://api.github.com/repos/owner/repo/actions/test"
        for destination in ("https://api.github.com/moved", "https://unrelated.invalid/moved", "http://unrelated.invalid/moved"):
            for code in (301, 302, 303, 307, 308):
                with self.subTest(destination=destination, code=code):
                    requests, policies = [], []

                    def respond(request):
                        requests.append((request.full_url, request.get_header("Authorization")))
                        headers = Message()
                        headers["Location"] = destination
                        response = urllib.response.addinfourl(io.BytesIO(b"{}"), headers, request.full_url,
                                                            code if len(requests) == 1 else 200)
                        response.msg = "fixture response"
                        return response

                    class FakeHTTPS(ci_gate.urllib.request.HTTPSHandler):
                        def https_open(self, request):
                            return respond(request)

                    class FakeHTTP(ci_gate.urllib.request.HTTPHandler):
                        def http_open(self, request):
                            return respond(request)

                    def build(*handlers):
                        policies.extend(handlers)
                        return original_builder(*handlers, FakeHTTPS(), FakeHTTP(), ci_gate.urllib.request.ProxyHandler({}))

                    with patch.object(ci_gate.urllib.request, "build_opener", side_effect=build):
                        with self.assertRaises(ci_gate.urllib.error.HTTPError) as caught:
                            ci_gate.request("owner/repo", "actions/test", "fake-test-token")
                    self.assertEqual(caught.exception.code, code)
                    caught.exception.close()
                    self.assertTrue(any(isinstance(handler, ci_gate.NoRedirect) for handler in policies))
                    self.assertEqual(requests, [(initial, "Bearer fake-test-token")])

    def test_only_exact_push_source_and_branch_are_eligible(self):
        self.assertIsNone(ci_gate.select_run(self.listing([]), SHA, "main"))
        for key, value in (("head_sha", "b" * 40), ("event", "pull_request"), ("head_branch", "other"), ("path", "other.yml")):
            run = dict(RUN, **{key: value})
            with self.subTest(key=key):
                self.assertIsNone(ci_gate.select_run(self.listing([run]), SHA, "main"))
        self.assertEqual(ci_gate.select_run(self.listing([RUN]), SHA, "main"), RUN)

    def test_latest_failed_run_is_not_replaced_with_older_success(self):
        failed = dict(RUN, id=124, conclusion="failure")
        self.assertEqual(ci_gate.select_run(self.listing([RUN, failed]), SHA, "main"), failed)

    def test_pagination_overflow_cannot_silently_hide_runs(self):
        with self.assertRaises(ValueError):
            ci_gate.select_run({"total_count": 101, "workflow_runs": []}, SHA, "main")

    def test_four_successful_expected_jobs_make_traceable_receipt(self):
        result = {"total_count": 4, "jobs": [{"name": name, "status": "completed", "conclusion": "success"} for name in ci_gate.JOBS]}
        receipt = ci_gate.validate_jobs(RUN, result, "owner/repo")
        self.assertEqual(receipt["head_sha"], SHA)
        self.assertEqual(set(receipt["jobs"]), ci_gate.JOBS)
        self.assertEqual(receipt["run_attempt"], 1)

    def test_missing_duplicate_skipped_failed_and_paginated_jobs_fail(self):
        valid = [{"name": name, "status": "completed", "conclusion": "success"} for name in sorted(ci_gate.JOBS)]
        variants = [[], valid[:3], valid + [valid[0]], [valid[0]] * 4]
        for conclusion in ("skipped", "failure", "cancelled", None):
            jobs = copy.deepcopy(valid)
            jobs[0]["conclusion"] = conclusion
            variants.append(jobs)
        for jobs in variants:
            with self.subTest(jobs=jobs), self.assertRaises(ValueError):
                ci_gate.validate_jobs(RUN, {"total_count": len(jobs), "jobs": jobs}, "owner/repo")
        with self.assertRaises(ValueError):
            ci_gate.validate_jobs(RUN, {"total_count": 5, "jobs": valid}, "owner/repo")


if __name__ == "__main__":
    unittest.main()
