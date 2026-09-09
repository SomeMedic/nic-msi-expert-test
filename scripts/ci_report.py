"""Collect bounded CI provenance and test counts without copying logs or failures."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
LOCKS = ("uv.lock", "apps/frontend/pnpm-lock.yaml", "models.lock.json",
         "parser-assets.lock.json", "infra/docker/requirements-ml-linux.lock",
         "infra/docker/requirements-parser-service.lock")
LANES = frozenset({"static", "contracts", "frontend", "integration", "smoke"})


def test_counts(path: Path) -> dict[str, int]:
    if path.is_symlink() or path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("INVALID_TEST_REPORT")
    body = path.read_bytes()
    if b"<!DOCTYPE" in body or b"<!ENTITY" in body:
        raise ValueError("INVALID_TEST_REPORT")
    document = ET.fromstring(body)
    suites = [document] if document.tag == "testsuite" else list(document.findall("testsuite"))
    if document.tag not in {"testsuite", "testsuites"} or not suites:
        raise ValueError("INVALID_TEST_REPORT")
    # Do not retain names, properties, captured output, error messages or XML text.
    return {key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
            for key in ("tests", "failures", "errors", "skipped")}


def verification_summary(directory: Path) -> dict:
    path = directory / "summary.json"
    if directory.is_symlink() or path.is_symlink() or path.stat().st_size > 1_000_000:
        raise ValueError("INVALID_VERIFICATION_REPORT")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != "verification.v1" or raw.get("status") not in {
        "passed", "failed", "interrupted", "running",
    }:
        raise ValueError("INVALID_VERIFICATION_REPORT")
    steps = []
    for row in raw["results"]:
        if row["lane"] not in LANES or type(row["exit_code"]) is not int:
            raise ValueError("INVALID_VERIFICATION_REPORT")
        seconds = row["duration_seconds"]
        if type(seconds) not in {int, float} or not 0 <= seconds < 86400:
            raise ValueError("INVALID_VERIFICATION_REPORT")
        steps.append({"lane": row["lane"], "exit_code": row["exit_code"], "duration_seconds": seconds})
    counts = {lane: test_counts(directory / f"{lane}.xml") for lane in LANES
              if (directory / f"{lane}.xml").is_file()}
    return {"verification_id": directory.name, "status": raw["status"], "steps": steps,
            "tests": counts, "real_model_quality_verified": False}


def collect(root: Path) -> dict:
    verification = root / ".cache" / "verification"
    if verification.is_symlink():
        raise ValueError("INVALID_VERIFICATION_REPORT")
    directories = sorted(path for path in verification.glob("*")
                         if re.fullmatch(r"[a-f0-9]{32}", path.name))
    if len(directories) > 100:
        raise ValueError("TOO_MANY_VERIFICATION_REPORTS")
    summaries = [verification_summary(path) for path in directories if (path / "summary.json").is_file()]
    revision = subprocess.run(["git", "rev-parse", "--verify", "HEAD"], cwd=root,
                              capture_output=True, text=True, check=False, timeout=10)
    commit = revision.stdout.strip() if revision.returncode == 0 else None
    if commit is not None and not re.fullmatch(r"[a-f0-9]{40,64}", commit):
        raise ValueError("INVALID_GIT_REVISION")
    return {
        "schema_version": "ci.evidence.v1", "git_commit": commit,
        "python": platform.python_version(), "platform": platform.system(),
        "runner_image": {key: os.environ[key] for key in ("ImageOS", "ImageVersion") if key in os.environ},
        "lock_sha256": {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in LOCKS},
        "verification": summaries,
        "real_model_quality_verified": False,
        "excluded_prepared_gates": ["real_indexing_service", "retrieval_composition", "tracing_delivery"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "ci-results") or output.suffix != ".json" or args.output.is_symlink():
        parser.error("output must be a JSON file inside ci-results")
    try:
        report = collect(ROOT)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError, KeyError, TypeError, ET.ParseError, subprocess.SubprocessError):
        print("CI_REPORT_FAILED", file=sys.stderr)
        return 1
    print("CI_REPORT_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
