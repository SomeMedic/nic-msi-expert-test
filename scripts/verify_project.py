"""Run local/CI checks with prepared dependencies; never download models or corpus.

The explicit integration lane uses the guarded project PostgreSQL/Redis/S3
fixtures. It excludes real-model and Collector deployment gates, which require
their own prepared environment and must be reported separately.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
LANES = ("static", "contracts", "frontend", "integration", "smoke")
SOURCE_PATHS = (
    "packages", "apps/backend/src", "apps/agent-runtime/src",
    "apps/ingestion-worker/src", "apps/retrieval-ml/src", "apps/outbox-publisher/src",
)
SEPARATE_GATES = (
    "tests/integration/test_real_indexing_service.py",
    "tests/integration/test_retrieval_composition.py",
    "tests/integration/test_tracing_delivery.py",
)
SMOKE_TESTS = (
    "tests/integration/test_answer_graph.py",
)


def commands(lane: str, output: Path) -> list[list[str]]:
    python = sys.executable
    if lane == "static":
        return [
            [python, "-m", "ruff", "check", "apps", "packages", "scripts", "tests"],
            [python, "-m", "mypy", "--explicit-package-bases", *SOURCE_PATHS,
             "scripts/export_openapi.py", "scripts/verify_project.py"],
            [python, "scripts/export_openapi.py", "--check"],
        ]
    if lane == "frontend":
        pnpm = shutil.which("pnpm.cmd" if os.name == "nt" else "pnpm") or "pnpm"
        return [[pnpm, "--dir", "apps/frontend", task] for task in ("lint", "test", "build")]
    if lane in {"contracts", "integration", "smoke"}:
        paths = list(SMOKE_TESTS) if lane == "smoke" else [f"tests/{'contract' if lane == 'contracts' else lane}"]
        arguments = [python, "-m", "pytest", *paths,
                     "-q", "--basetemp", str(output / f"tmp-{lane}"),
                     "--junitxml", str(output / f"{lane}.xml")]
        if lane == "integration":
            arguments += [f"--ignore={path}" for path in SEPARATE_GATES]
        return [arguments]
    raise ValueError("UNKNOWN_VERIFICATION_LANE")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane", action="append", choices=LANES,
                        help="repeat for multiple lanes; default: static, contracts, frontend")
    parser.add_argument("--plan", action="store_true", help="show commands without executing or writing files")
    args = parser.parse_args(argv)
    lanes = list(dict.fromkeys(args.lane or LANES[:3]))
    output = ROOT / ".cache" / "verification" / uuid4().hex
    plan = [{"lane": lane, "commands": commands(lane, output)} for lane in lanes]
    report: dict = {
        "schema_version": "verification.v1", "plan": plan, "status": "planned",
        "real_model_quality_verified": False,
        "separate_integration_gates": list(SEPARATE_GATES), "results": [],
    }
    if args.plan:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    output.mkdir(parents=True, exist_ok=False)
    report["started_at"] = datetime.now(timezone.utc).isoformat()
    environment = {key: value for key, value in os.environ.items() if not key.startswith("EXPERT_")}
    environment["PYTHONUTF8"] = "1"
    report["status"] = "running"
    try:
        for item in plan:
            current_environment = environment.copy()
            if item["lane"] in {"integration", "smoke"}:
                current_environment.update(EXPERT_INTEGRATION_TESTS="1", EXPERT_P03_RESOURCE_TESTS="1")
            for command in item["commands"]:
                print(json.dumps({"lane": item["lane"], "command": command}), flush=True)
                started = time.monotonic()
                try:
                    result = subprocess.run(command, cwd=ROOT, env=current_environment, check=False)
                    code = result.returncode
                except OSError:
                    print("VERIFICATION_EXECUTABLE_UNAVAILABLE", flush=True)
                    code = 127
                report["results"].append({"lane": item["lane"], "command": command,
                    "exit_code": code, "duration_seconds": round(time.monotonic() - started, 3)})
                if code:
                    report["status"] = "failed"
                    return 1
        report["status"] = "passed"
        return 0
    except KeyboardInterrupt:
        report["status"] = "interrupted"
        return 130
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        (output / "summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"verification_status": report["status"], "report": str(output / "summary.json")}))


if __name__ == "__main__":
    raise SystemExit(main())
