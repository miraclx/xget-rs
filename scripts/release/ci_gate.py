"""Wait for CI on the candidate's exact push SHA, never a nearby commit or PR head."""

import argparse
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

JOBS = {"rustfmt (nightly)", "clippy", "test", "windows check"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--deadline-seconds", type=int, default=5400)
    args = parser.parse_args()
    if not 1 <= args.deadline_seconds <= 5400:
        raise ValueError("deadline must be between 1 and 5400 seconds")
    repository = os.environ["GITHUB_REPOSITORY"]
    sha = os.environ["GITHUB_SHA"]
    branch = os.environ["GITHUB_REF_NAME"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) or not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("invalid repository or source SHA")
    deadline = time.monotonic() + args.deadline_seconds
    token = os.environ["GH_TOKEN"]
    query = urllib.parse.urlencode({"head_sha": sha, "event": "push", "per_page": 100})
    while time.monotonic() < deadline:
        try:
            listing = request(repository, f"actions/workflows/ci.yml/runs?{query}", token)
            run = select_run(listing, sha, branch)
            if run is not None and run["status"] == "completed":
                if run["conclusion"] != "success":
                    raise ValueError("exact-source CI did not succeed; inspect its Actions run")
                jobs = request(repository, f"actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100", token)
                receipt = validate_jobs(run, jobs, repository)
                args.output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                return
        except urllib.error.HTTPError as error:
            if error.code not in (429, 500, 502, 503, 504):
                raise
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(min(30, max(0, deadline - time.monotonic())))
    raise TimeoutError("exact-source CI was not successful before the bounded deadline")


def request(repository, path, token):
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}/{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "xget-rc-ci-gate"},
    )
    opener = urllib.request.build_opener(NoRedirect())
    with opener.open(request, timeout=30) as response:
        data = response.read(4 * 1024 * 1024 + 1)
        if len(data) > 4 * 1024 * 1024:
            raise ValueError("Actions response exceeded bound")
        return json.loads(data)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """A repository move must not route the Actions credential to any redirected URL."""
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def select_run(listing, sha, branch):
    if listing["total_count"] > 100 or len(listing["workflow_runs"]) > 100:
        raise ValueError("too many exact-source runs; inspect before accepting")
    matching = [run for run in listing["workflow_runs"]
                if run["head_sha"] == sha and run["event"] == "push"
                and run["head_branch"] == branch and run["path"] == ".github/workflows/ci.yml"]
    return max(matching, key=lambda run: (run["id"], run["run_attempt"]), default=None)


def validate_jobs(run, result, repository):
    jobs = result["jobs"]
    names = [job["name"] for job in jobs]
    if (run["status"] != "completed" or run["conclusion"] != "success"
            or result["total_count"] != len(jobs) or len(names) != len(JOBS) or set(names) != JOBS
            or any(job["status"] != "completed" or job["conclusion"] != "success" for job in jobs)):
        raise ValueError("exact-source CI jobs are missing, duplicate, skipped or unsuccessful")
    return {
        "repository": repository, "head_sha": run["head_sha"], "event": run["event"],
        "path": run["path"], "conclusion": run["conclusion"], "run_id": run["id"],
        "run_attempt": run["run_attempt"], "html_url": run["html_url"],
        "jobs": {job["name"]: job["conclusion"] for job in jobs},
        "provider_evidence": "source CI all-feature loopback/unit fixtures, not live services on every native host",
    }


if __name__ == "__main__":
    main()
