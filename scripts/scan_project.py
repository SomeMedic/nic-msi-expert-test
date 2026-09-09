"""Plan by default; run a pinned local Trivy without mounting the repository/socket."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
import threading
import time
from typing import Any
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
TRIVY_VERSION = "0.74.0"
TRIVY_IMAGE = "aquasec/trivy@sha256:ee940acbf1f58ebadb42d01434ce4609530bf1b52536afbd1eee66cd7123c5c9"
SEVERITIES = ("UNKNOWN", "LOW", "MEDIUM", "HIGH", "CRITICAL")
SCANNERS = ("vuln", "misconfig", "secret")
DIRECTORIES = ("apps", "packages", "scripts", "tests", "infra", "config", ".github", "contracts")
EXCLUDED = frozenset({"docs", "corpus", "evals", "evidence", "models", "data", "tmp", "dist", "build",
    "node_modules", "__pycache__", ".venv", ".venv-ml", ".git", ".cache", ".secrets", ".omx", ".next",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", ".pnpm-store", "playwright-report", "test-results"})
EXTENSIONS = frozenset({".py", ".ts", ".tsx", ".js", ".mjs", ".cjs", ".json", ".yaml", ".yml", ".toml",
    ".sh", ".ps1", ".sql", ".css", ".html", ".vue", ".lock"})
ROOT_FILES = ("pyproject.toml", "uv.lock", "compose.yaml", ".env.example", ".dockerignore", ".gitignore",
              ".python-version", "Makefile")
ALIASES = {"infra/docker/requirements-ml-linux.lock": "production/ml/requirements.txt",
           "infra/docker/requirements-parser-service.lock": "production/parser/requirements.txt"}
REQUIRED = {"uv.lock", "pyproject.toml", "compose.yaml", *ALIASES,
            "apps/frontend/package.json", "apps/frontend/pnpm-lock.yaml"}
MAX_FILE = 16 * 1024 * 1024
MAX_STAGE = 128 * 1024 * 1024
MAX_REPORT = 128 * 1024 * 1024
IMAGE_SCAN_PRIVATE_TMP_PREFLIGHT_RESERVE_BYTES = 16 * 1024**3
IMAGE_SCAN_HOST_FREE_FLOOR_BYTES = 16 * 1024**3
HOST_FREE_WATCHDOG_INTERVAL_SECONDS = 5.0


COMMAND_FAILURE_REASONS = frozenset({"HOST_COMMAND_TIMEOUT", "SCANNER_REPORTED_DEADLINE",
    "SCANNER_REPORTED_NO_SPACE", "SCANNER_REPORTED_MEMORY", "SCANNER_REPORTED_CACHE_LOCK",
    "SCANNER_REPORTED_DB_SCHEMA", "SCANNER_REPORTED_DB_DOWNLOAD", "SCANNER_REPORTED_SCANNER_INIT",
    "SCANNER_REPORTED_IMAGE_SOURCE_UNAVAILABLE", "SCANNER_REPORTED_LAYER_READ",
    "SCANNER_REPORTED_ANALYSIS", "HOST_FREE_SPACE_WATCHDOG", "UNCLASSIFIED_COMMAND_FAILURE"})
SCANNER_ERROR_CATEGORIES = COMMAND_FAILURE_REASONS | frozenset({"SCANNER_REPORTED_TIMEOUT_HINT",
    "SCANNER_REPORTED_ANALYSIS_CONCURRENCY"})


def safe_command_diagnostics(value):
    if not isinstance(value, dict):
        return None
    result: dict[str, Any] = {}
    for key in ("exit_code", "stderr_total_bytes", "stderr_tail_bytes"):
        item = value.get(key)
        if type(item) is int and (-255 if key == "exit_code" else 0) <= item <= 2**32 - 1:
            result[key] = item
    reason = value.get("reason")
    if isinstance(reason, str) and reason in COMMAND_FAILURE_REASONS:
        result["reason"] = reason
    categories = value.get("scanner_error_categories")
    if (isinstance(categories, list) and categories and len(categories) <= 8
            and all(isinstance(item, str) and item in SCANNER_ERROR_CATEGORIES for item in categories)):
        result["scanner_error_categories"] = sorted(set(categories))
    sha = value.get("stderr_tail_sha256")
    if isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{64}", sha):
        result["stderr_tail_sha256"] = sha
    return result or None


class ScanError(Exception):
    def __init__(self, code: str, *, diagnostics=None):
        self.code = code
        self.diagnostics = safe_command_diagnostics(diagnostics)
        super().__init__(code)


def require(condition: bool, code: str):
    if not condition:
        raise ScanError(code)


def digest(path: Path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def json_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def safe_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    created = False
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            created = True
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if created:
            temporary.unlink(missing_ok=True)


def safe_file_presence(path: Path, *, maximum: int = MAX_REPORT):
    if path.is_file() and not path.is_symlink() and not path.is_junction():
        size = path.stat().st_size
        if 0 < size <= maximum:
            return {"present": True, "size_bytes": size, "sha256": digest(path)}
        return {"present": True, "size_bytes": size, "status": "size_out_of_bounds"}
    return {"present": False}


def regular_source(path: Path, root: Path):
    info = path.lstat()
    require(not path.is_symlink() and not path.is_junction() and stat.S_ISREG(info.st_mode)
            and info.st_nlink == 1 and path.resolve().is_relative_to(root), "UNSAFE_SOURCE_PATH")
    require(info.st_size <= MAX_FILE, "SOURCE_FILE_TOO_LARGE")
    return info


def selected_files(root: Path) -> list[Path]:
    root = root.resolve()
    selected = [root / name for name in ROOT_FILES if (root / name).is_file()]
    for directory in DIRECTORIES:
        base = root / directory
        if not base.exists():
            continue
        require(not base.is_symlink() and not base.is_junction(), "UNSAFE_SOURCE_PATH")
        for folder, dirs, files in os.walk(base, followlinks=False):
            parent = Path(folder)
            dirs[:] = [name for name in dirs if name not in EXCLUDED and not name.startswith(".venv")]
            for name in dirs:
                require(not (parent / name).is_symlink() and not (parent / name).is_junction(), "UNSAFE_SOURCE_PATH")
            for name in files:
                path = parent / name
                # This specific configuration embeds a source raster review; it
                # belongs to the private corpus boundary, not source scan staging.
                if path.relative_to(root).as_posix() == "config/parsing/region-reviews.json":
                    continue
                if name.startswith(".env") and name != ".env.example":
                    continue
                if path.suffix.lower() in EXTENSIONS or name == "Dockerfile" or name.startswith("Dockerfile."):
                    selected.append(path)
    selected = sorted(set(selected))
    paths = {p.relative_to(root).as_posix() for p in selected}
    require(REQUIRED <= paths, "REQUIRED_SCAN_INPUT_MISSING")
    require(len(selected) <= 8000, "SOURCE_FILE_COUNT_LIMIT")
    require(sum(regular_source(p, root).st_size for p in selected) <= MAX_STAGE, "SOURCE_TOTAL_LIMIT")
    return selected


def source_manifest(root: Path, files: list[Path]):
    return [{"path": p.relative_to(root).as_posix(), "size_bytes": p.stat().st_size, "sha256": digest(p)} for p in files]


def stage(root: Path, destination: Path, manifest):
    destination.mkdir()
    mapping = {}
    for row in manifest:
        source = root / row["path"]
        regular_source(source, root)
        target = ALIASES.get(row["path"], row["path"])
        path = destination / target
        require(path.resolve().is_relative_to(destination.resolve()), "UNSAFE_STAGE_PATH")
        path.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as stream, path.open("xb") as output:
            shutil.copyfileobj(stream, output, length=1024 * 1024)
        require(path.stat().st_size == row["size_bytes"] and digest(path) == row["sha256"], "SOURCE_CHANGED_DURING_STAGE")
        mapping[target] = row["path"]
    return mapping


def image_tag(value: str):
    require(re.fullmatch(r"[a-z0-9][a-z0-9._/-]*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", value) is not None
            and not value.endswith(":latest"), "EXACT_LOCAL_IMAGE_TAG_REQUIRED")
    return value


def plan(root: Path, image: str | None, *, scan_timeout_seconds: int | None = None):
    if scan_timeout_seconds is not None:
        require(bool(image) and type(scan_timeout_seconds) is int
                and 1200 <= scan_timeout_seconds <= 2400, "IMAGE_SCAN_TIMEOUT_INVALID")
    timeout = scan_timeout_seconds if scan_timeout_seconds is not None else 1200 if image else 480
    manifest = [] if image else source_manifest(root, selected_files(root))
    return {"schema_version": "p12.security-scan.v1", "state": "planned", "executed": False,
        "mode": "image" if image else "filesystem", "image_tag": image_tag(image) if image else None,
        "scanner_image": TRIVY_IMAGE, "scanner_version": TRIVY_VERSION, "platform": "linux/amd64",
        "scanners": list(SCANNERS), "fail_severities": ["HIGH", "CRITICAL"], "severity_filter": list(SEVERITIES),
        "source_file_count": len(manifest), "source_bytes": sum(r["size_bytes"] for r in manifest),
        "source_manifest_sha256": json_digest(manifest), "source_manifest": manifest,
        "source_selection": "explicit source directories including current untracked work; no corpus/docs/cache/secrets",
        "source_exclusions": sorted(EXCLUDED) + ["config/parsing/region-reviews.json", "PDF/binary/media files", ".env except .env.example"],
        "production_lock_aliases": ALIASES, "license_gate": "not_implemented_in_this_tool",
        "scan_network": "none; only a separate source-free DB download container has network",
        "scan_tmpfs_limit_bytes": None if image else 256 * 1024**2,
        "scan_private_tmp_preflight_reserve_bytes": IMAGE_SCAN_PRIVATE_TMP_PREFLIGHT_RESERVE_BYTES if image else None,
        "scan_host_free_watchdog_floor_bytes": IMAGE_SCAN_HOST_FREE_FLOOR_BYTES if image else None,
        "scan_tmp_storage": {"target": "/tmp", "mode": "private_disk_backed_bind",
                             "preflight_reserve_bytes": IMAGE_SCAN_PRIVATE_TMP_PREFLIGHT_RESERVE_BYTES,
                             "disk_limit_enforced": False} if image else {
            "target": "/tmp", "mode": "tmpfs", "budget_bytes": 256 * 1024**2, "disk_limit_enforced": True},
        "scan_timeout_seconds": timeout,
        "scan_cpu_limit": 2, "scan_memory_limit_bytes": 2 * 1024**3, "scan_parallelism": 2,
        "checks_bundle": {"mode": "embedded in pinned scanner image", "updates_disabled": True,
                          "scanner_image": TRIVY_IMAGE},
        "limitations": ["Trivy has no Docker Compose misconfiguration scanner; Compose policy review is separate.",
            "No license acceptance policy or real-model/corpus evaluation is performed.",
            "Source vulnerability coverage is locks; installed environment/base OS coverage requires image scans."]}


def scanner_error_categories(tail: bytes, *, timed_out: bool = False) -> list[str]:
    if timed_out:
        return ["HOST_COMMAND_TIMEOUT"]
    fatal = b"\n".join(line.lower() for line in tail.splitlines()
                       if any(level in line.lower() for level in (b"fatal", b"error", b"warn")))
    markers = (
        (b"increase --timeout value", "SCANNER_REPORTED_TIMEOUT_HINT"),
        (b"context deadline exceeded", "SCANNER_REPORTED_DEADLINE"),
        (b"no space left on device", "SCANNER_REPORTED_NO_SPACE"),
        (b"cannot allocate memory", "SCANNER_REPORTED_MEMORY"),
        (b"out of memory", "SCANNER_REPORTED_MEMORY"),
        (b"cache may be in use", "SCANNER_REPORTED_CACHE_LOCK"),
        (b"old db schema", "SCANNER_REPORTED_DB_SCHEMA"),
        (b"failed to download vulnerability db", "SCANNER_REPORTED_DB_DOWNLOAD"),
        (b"unable to initialize a scanner", "SCANNER_REPORTED_SCANNER_INIT"),
        (b"unable to initialize an image scanner", "SCANNER_REPORTED_SCANNER_INIT"),
        (b"unable to inspect the image", "SCANNER_REPORTED_IMAGE_SOURCE_UNAVAILABLE"),
        (b"cannot connect to the docker daemon", "SCANNER_REPORTED_IMAGE_SOURCE_UNAVAILABLE"),
        (b"failed to analyze layer", "SCANNER_REPORTED_LAYER_READ"),
        (b"unable to get uncompressed layer", "SCANNER_REPORTED_LAYER_READ"),
        (b"not found in tar", "SCANNER_REPORTED_LAYER_READ"),
        (b"failed analysis", "SCANNER_REPORTED_ANALYSIS"),
        (b"analyze error", "SCANNER_REPORTED_ANALYSIS"),
        (b"semaphore acquire", "SCANNER_REPORTED_ANALYSIS_CONCURRENCY"),
    )
    categories = [category for marker, category in markers if marker in fatal]
    return sorted(set(categories)) or ["UNCLASSIFIED_COMMAND_FAILURE"]


def primary_failure_reason(categories: list[str]) -> str:
    for category in ("HOST_COMMAND_TIMEOUT", "HOST_FREE_SPACE_WATCHDOG", "SCANNER_REPORTED_NO_SPACE",
            "SCANNER_REPORTED_MEMORY", "SCANNER_REPORTED_DEADLINE", "SCANNER_REPORTED_CACHE_LOCK",
            "SCANNER_REPORTED_DB_SCHEMA", "SCANNER_REPORTED_DB_DOWNLOAD", "SCANNER_REPORTED_SCANNER_INIT",
            "SCANNER_REPORTED_IMAGE_SOURCE_UNAVAILABLE", "SCANNER_REPORTED_LAYER_READ",
            "SCANNER_REPORTED_ANALYSIS"):
        if category in categories:
            return category
    return "UNCLASSIFIED_COMMAND_FAILURE"


def stderr_diagnostics(stream, exit_code: int | None, *, timed_out: bool = False):
    size = stream.tell()
    stream.seek(max(0, size - 65536))
    tail = stream.read(65536)
    # Only report fixed scanner failure categories. The original text can
    # contain paths, matches and credentials and must never escape this scope.
    categories = scanner_error_categories(tail, timed_out=timed_out)
    reason = primary_failure_reason(categories)
    return safe_command_diagnostics({"exit_code": exit_code, "reason": reason,
        "scanner_error_categories": categories,
        "stderr_total_bytes": size, "stderr_tail_bytes": len(tail),
        "stderr_tail_sha256": hashlib.sha256(tail).hexdigest()})


def command(arguments: list[str], *, timeout: int = 600) -> bytes:
    # Do not inherit TRIVY_*, proxy, application credentials or Docker endpoint
    # overrides. Docker's chosen local context is checked and then explicit.
    allowed = ("PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "HOME", "USERPROFILE", "LOCALAPPDATA", "APPDATA")
    environment = {key: os.environ[key] for key in allowed if key in os.environ}
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        try:
            result = subprocess.run(arguments, stdout=stdout, stderr=stderr, stdin=subprocess.DEVNULL,
                timeout=timeout, env=environment, check=False, close_fds=True)
        except subprocess.TimeoutExpired:
            raise ScanError("SCANNER_COMMAND_TIMEOUT",
                diagnostics=stderr_diagnostics(stderr, None, timed_out=True)) from None
        except OSError:
            raise ScanError("SCANNER_COMMAND_UNAVAILABLE") from None
        if result.returncode != 0:
            raise ScanError("SCANNER_COMMAND_FAILED", diagnostics=stderr_diagnostics(stderr, result.returncode))
        require(stdout.tell() <= 1024 * 1024, "SCANNER_COMMAND_OUTPUT_LIMIT")
        stdout.seek(0)
        return stdout.read()


def local_docker(run):
    context = run(["docker", "context", "show"], timeout=30).decode().strip()
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", context) is not None, "DOCKER_CONTEXT_INVALID")
    data = json.loads(run(["docker", "context", "inspect", context], timeout=30))
    require(isinstance(data, list) and len(data) == 1, "DOCKER_CONTEXT_INVALID")
    host = data[0]["Endpoints"]["docker"]["Host"]
    require(isinstance(host, str) and (host.startswith("npipe:////./pipe/") or host.startswith("unix:///")),
            "LOCAL_DOCKER_REQUIRED")
    return ["docker", "--context", context]


def mount(path: Path, target: str, readonly=False):
    value = str(path.resolve())
    require("," not in value and "\n" not in value and "\r" not in value, "MOUNT_PATH_INVALID")
    return ["--mount", f"type=bind,source={value},target={target}" + (",readonly" if readonly else "")]


def directory_size_bytes(path: Path):
    total = 0
    entries = 0
    if not path.exists():
        return 0
    if path.is_file():
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and not path.is_symlink() and not path.is_junction(),
                "STORAGE_TELEMETRY_SCOPE_INVALID")
        return info.st_size
    require(path.is_dir() and not path.is_symlink() and not path.is_junction(), "STORAGE_TELEMETRY_SCOPE_INVALID")
    resolved_root = path.resolve()
    for folder, dirs, files in os.walk(path, followlinks=False):
        parent = Path(folder)
        require(parent.resolve().is_relative_to(resolved_root), "STORAGE_TELEMETRY_SCOPE_INVALID")
        for name in dirs:
            item = parent / name
            require(not item.is_symlink() and not item.is_junction(), "STORAGE_TELEMETRY_SCOPE_INVALID")
        for name in files:
            item = parent / name
            info = item.lstat()
            require(stat.S_ISREG(info.st_mode) and not item.is_symlink() and not item.is_junction(),
                    "STORAGE_TELEMETRY_SCOPE_INVALID")
            total += info.st_size
            entries += 1
            require(entries <= 200000 and total <= 64 * 1024**3, "STORAGE_TELEMETRY_SCOPE_INVALID")
    return total


def fixed_mount_storage_bytes(workspace: Path, *, input_archive: Path | None, image_scan: bool):
    workspace = workspace.resolve()
    require(bool(workspace.name) and workspace.parent.name == "security-scans" and workspace.is_dir(),
            "STORAGE_TELEMETRY_SCOPE_INVALID")
    mounts: dict[str, dict[str, Any]] = {}
    targets = [("/cache", workspace / "cache", "bind_dir"), ("/results", workspace / "raw", "bind_dir")]
    if image_scan:
        targets.append(("/tmp", workspace / "tmp", "private_disk_backed_bind"))
    else:
        mounts["/tmp"] = {"kind": "tmpfs", "budget_bytes": 256 * 1024**2}
    if input_archive is not None:
        targets.append(("/input", input_archive, "readonly_bind_file"))
    for target, mounted_path, kind in targets:
        resolved = mounted_path.resolve()
        require((resolved == workspace or resolved.is_relative_to(workspace)) and mounted_path.exists(),
                "STORAGE_TELEMETRY_SCOPE_INVALID")
        usage = shutil.disk_usage(mounted_path if mounted_path.is_dir() else mounted_path.parent)
        mounts[target] = {"kind": kind, "total_bytes": usage.total, "used_bytes": usage.used,
                          "free_bytes": usage.free, "content_bytes": directory_size_bytes(mounted_path)}
        if target == "/tmp" and image_scan:
            mounts[target]["preflight_reserve_bytes"] = IMAGE_SCAN_PRIVATE_TMP_PREFLIGHT_RESERVE_BYTES
            mounts[target]["disk_limit_enforced"] = False
    require(set(mounts) <= {"/tmp", "/cache", "/results", "/input"}, "STORAGE_TELEMETRY_SCOPE_INVALID")
    return {"schema_version": "p12.scan-storage-telemetry.v1", "mounts": mounts}


def container_args(docker, workspace: Path, name: str, owner: str, *, network: str,
                   image_scan: bool = False, retain_for_diagnostics: bool = False):
    args = [*docker, "run", *([] if retain_for_diagnostics else ["--rm"]),
        "--pull", "never", "--platform", "linux/amd64", "--name", name,
        "--label", "expert.scan.owner=" + owner, "--network", network, "--read-only", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--pids-limit", "128", "--cpus", "2", "--memory", "2g",
        "--memory-swap", "2g"]
    if image_scan:
        args += mount(workspace / "tmp", "/tmp")
    else:
        args += ["--tmpfs", "/tmp:rw,nosuid,noexec,size=268435456"]
    return [*args, *mount(workspace / "cache", "/cache")]


def db_attestation(cache: Path):
    database, metadata_path = cache / "db/trivy.db", cache / "db/metadata.json"
    require(database.is_file() and 0 < database.stat().st_size <= 2 * 1024**3
            and metadata_path.is_file() and metadata_path.stat().st_size < 16384, "VULNERABILITY_DB_MISSING")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    require(metadata.get("Version") == 2, "VULNERABILITY_DB_SCHEMA")
    dates = {}
    for field in ("UpdatedAt", "NextUpdate", "DownloadedAt"):
        value = metadata.get(field)
        require(isinstance(value, str) and len(value) < 80, "VULNERABILITY_DB_METADATA")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        require(parsed.tzinfo is not None, "VULNERABILITY_DB_METADATA")
        dates[field] = parsed.astimezone(timezone.utc).isoformat()
    require(datetime.fromisoformat(dates["NextUpdate"]) > datetime.now(timezone.utc), "VULNERABILITY_DB_EXPIRED")
    return {"schema_version": 2, **dates, "sha256": digest(database), "size_bytes": database.stat().st_size,
            "metadata_sha256": digest(metadata_path)}


def identifier(value, *, fallback="INVALID_IDENTIFIER"):
    return value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/@+ ,*()=~-]{0,199}", value) else fallback


def summarize(path: Path, mapping: dict[str, str], *, image: bool):
    require(path.is_file() and 0 < path.stat().st_size <= MAX_REPORT, "SCAN_REPORT_MISSING_OR_OVERSIZED")
    raw_sha = digest(path)
    report = json.loads(path.read_text(encoding="utf-8"))
    require(report.get("SchemaVersion") == 2 and isinstance(report.get("Results"), list), "SCAN_REPORT_INVALID")
    counts = {kind: dict.fromkeys(SEVERITIES, 0) for kind in SCANNERS}
    findings: list[dict[str, Any]] = []
    targets: set[str] = set()
    config_targets: set[str] = set()
    package_counts: dict[str, int] = {}
    package_targets: set[str] = set()
    for result in report["Results"]:
        target = result.get("Target", "")
        require(isinstance(target, str), "SCAN_TARGET_INVALID")
        target = target.removeprefix("/scan/").removeprefix("./")
        if image:
            # Image paths can include sensitive filenames. They remain private;
            # the target digest distinguishes observations without printing them.
            safe_target = "image-target-" + hashlib.sha256(target.encode()).hexdigest()[:16]
        else:
            require(target in mapping, "SCAN_TARGET_OUTSIDE_STAGE")
            safe_target = mapping[target]
        targets.add(target)
        if result.get("Class") == "config":
            config_targets.add(target)
        if result.get("Class") in {"lang-pkgs", "os-pkgs"}:
            require(isinstance(result.get("Packages"), list), "PACKAGE_INVENTORY_MISSING")
            package_counts[safe_target] = len(result["Packages"])
            if result["Packages"]:
                package_targets.add(target)
        for kind, field in (("vuln", "Vulnerabilities"), ("misconfig", "Misconfigurations"), ("secret", "Secrets")):
            rows = result.get(field) or []
            require(isinstance(rows, list), "SCAN_FINDINGS_INVALID")
            for finding in rows:
                if kind == "misconfig" and finding.get("Status") != "FAIL":
                    require(finding.get("Status") in {"PASS", "EXCEPTION"}, "MISCONFIG_STATUS_INVALID")
                    continue
                severity = finding.get("Severity")
                require(severity in SEVERITIES, "SCAN_SEVERITY_INVALID")
                counts[kind][severity] += 1
                row = {"kind": kind, "target": safe_target, "severity": severity}
                if kind == "vuln":
                    row.update(id=identifier(finding.get("VulnerabilityID")), package=identifier(finding.get("PkgName")),
                        installed_version=identifier(finding.get("InstalledVersion")),
                        fixed_version=identifier(finding.get("FixedVersion"), fallback="not_reported"))
                else:
                    row["id"] = identifier(finding.get("ID") if kind == "misconfig" else finding.get("RuleID"))
                    line = finding.get("StartLine") if kind == "secret" else (finding.get("CauseMetadata") or {}).get("StartLine")
                    row["line"] = line if type(line) is int and 0 <= line <= 1000000 else None
                # No Title, Description, Message, Match, Code, CauseMetadata,
                # references, environment, scanner stderr or matched excerpt.
                findings.append(row)
                require(len(findings) <= 100000, "SCAN_FINDING_COUNT_LIMIT")
    require(bool(package_counts) and sum(package_counts.values()) > 0, "VULNERABILITY_COVERAGE_MISSING")
    if not image:
        required_targets = {"uv.lock", "apps/frontend/pnpm-lock.yaml", *ALIASES.values()}
        require(required_targets <= package_targets, "LOCK_SCAN_COVERAGE_MISSING")
        dockerfiles = {key for key in mapping if Path(key).name == "Dockerfile" or Path(key).name.startswith("Dockerfile.")}
        require(bool(dockerfiles) and dockerfiles <= config_targets, "DOCKERFILE_SCAN_COVERAGE_MISSING")
    high = sum(counts[kind][severity] for kind in SCANNERS for severity in ("HIGH", "CRITICAL"))
    return {"raw_report_sha256": raw_sha, "counts": counts, "finding_count": len(findings), "findings": findings,
        "high_critical_count": high, "package_counts_by_target": package_counts,
        "gate_passed": high == 0, "target_count": len(targets), "misconfig_target_count": len(config_targets)}


def cleanup_container(docker, name: str, owner: str, run):
    current = run([*docker, "container", "ls", "--all", "--filter", "label=expert.scan.owner=" + owner,
                   "--filter", "name=^/" + name + "$", "--format", "{{.Names}}"], timeout=30).decode().splitlines()
    if not current:
        return  # Verified absent; daemon connection failures are not absence.
    require(current == [name], "SCAN_CONTAINER_OWNERSHIP_CHANGED")
    labels = json.loads(run([*docker, "inspect", "--format", "{{json .Config.Labels}}", name], timeout=30))
    require(labels.get("expert.scan.owner") == owner, "SCAN_CONTAINER_OWNERSHIP_CHANGED")
    run([*docker, "rm", "--force", name], timeout=30)
    remaining = run([*docker, "container", "ls", "--all", "--filter", "label=expert.scan.owner=" + owner,
        "--filter", "name=^/" + name + "$", "--format", "{{.Names}}"], timeout=30).decode().splitlines()
    require(not remaining, "SCAN_CONTAINER_CLEANUP_NOT_CONFIRMED")


def scanner_exit_metadata(docker, name: str, owner: str, run):
    current = run([*docker, "container", "ls", "--all", "--filter", "label=expert.scan.owner=" + owner,
        "--filter", "name=^/" + name + "$", "--format", "{{.Names}}"], timeout=30).decode().splitlines()
    if not current:
        return {"available": False, "reason": "CONTAINER_NOT_PRESENT"}
    require(current == [name], "SCAN_CONTAINER_OWNERSHIP_CHANGED")
    # Explicit Go projection excludes State.Error, env, mounts and arbitrary labels.
    template = ('{"owner":{{json (index .Config.Labels "expert.scan.owner")}},'
        '"status":{{json .State.Status}},"running":{{json .State.Running}},'
        '"oom_killed":{{json .State.OOMKilled}},"exit_code":{{json .State.ExitCode}}}')
    data = json.loads(run([*docker, "inspect", "--format", template, name], timeout=30))
    require(isinstance(data, dict) and data.get("owner") == owner, "SCAN_CONTAINER_OWNERSHIP_CHANGED")
    require(isinstance(data.get("status"), str)
        and data["status"] in {"created", "running", "paused", "restarting", "removing", "exited", "dead"}
        and type(data.get("running")) is bool and type(data.get("oom_killed")) is bool
        and type(data.get("exit_code")) is int and 0 <= data["exit_code"] <= 255,
        "SCAN_EXIT_METADATA_INVALID")
    return {"available": True, **{key: data[key] for key in ("status", "running", "oom_killed", "exit_code")}}


def stop_owned_scan_container(docker, name: str, owner: str, run):
    current = run([*docker, "container", "ls", "--all", "--filter", "label=expert.scan.owner=" + owner,
        "--filter", "name=^/" + name + "$", "--format", "{{.Names}}"], timeout=30).decode().splitlines()
    if not current:
        return False
    require(current == [name], "SCAN_CONTAINER_OWNERSHIP_CHANGED")
    labels = json.loads(run([*docker, "inspect", "--format", "{{json .Config.Labels}}", name], timeout=30))
    require(labels.get("expert.scan.owner") == owner, "SCAN_CONTAINER_OWNERSHIP_CHANGED")
    run([*docker, "stop", "--time", "10", name], timeout=30)
    return True


def run_scan_with_host_free_watchdog(arguments: list[str], *, docker, name: str, owner: str, workspace: Path,
                                     run, timeout: int):
    triggered: list[int] = []
    stop = threading.Event()
    interval = HOST_FREE_WATCHDOG_INTERVAL_SECONDS

    def observe():
        while not stop.wait(interval):
            try:
                free = shutil.disk_usage(workspace).free
                if free >= IMAGE_SCAN_HOST_FREE_FLOOR_BYTES:
                    continue
                triggered.append(free)
                stop_owned_scan_container(docker, name, owner, run)
                return
            except Exception:
                return

    thread = threading.Thread(target=observe, daemon=True)
    thread.start()
    try:
        result = run(arguments, timeout=timeout)
    except Exception as error:
        stop.set()
        thread.join(timeout=1)
        if triggered:
            raise ScanError("SCANNER_COMMAND_FAILED", diagnostics={
                "reason": "HOST_FREE_SPACE_WATCHDOG", "exit_code": -1}) from None
        raise error
    stop.set()
    thread.join(timeout=1)
    if triggered:
        raise ScanError("SCANNER_COMMAND_FAILED", diagnostics={
            "reason": "HOST_FREE_SPACE_WATCHDOG", "exit_code": -1})
    return result


def cleanup_export_temporaries(root: Path, workspace: Path, owner: str):
    # Docker save may leave an unrenamed temporary after interruption. This
    # UUID directory was exclusively created by execute, never an app directory.
    require(re.fullmatch(r"[0-9a-f]{32}", owner) is not None
            and workspace == root / ".cache/security-scans" / owner
            and workspace.resolve() == workspace and workspace.is_dir(), "EXPORT_CLEANUP_SCOPE_INVALID")
    for path in workspace.iterdir():
        if re.fullmatch(r"\.docker_temp_[0-9]+", path.name) is None:
            continue
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and not path.is_symlink() and not path.is_junction()
                and info.st_nlink == 1 and path.resolve().parent == workspace, "EXPORT_CLEANUP_SCOPE_INVALID")
        path.unlink()


def execute(root: Path, output: Path, image: str | None, *, run=command,
            scan_timeout_seconds: int | None = None):
    root = root.resolve()
    output = (root / output).resolve()
    require(output.suffix == ".json" and any(output.is_relative_to(root / base) for base in
            ("ci-results", "docs/implementation/evidence/p12-scans", ".cache/security-scans")), "SCAN_OUTPUT_SCOPE_INVALID")
    report = plan(root, image, scan_timeout_seconds=scan_timeout_seconds)
    require(not output.exists(), "SCAN_OUTPUT_ALREADY_EXISTS")
    owner = uuid4().hex
    workspace = root / ".cache/security-scans" / owner
    workspace.mkdir(parents=True)
    for name in ("cache", "raw"):
        (workspace / name).mkdir()
    if image:
        (workspace / "tmp").mkdir()
    docker: list[str] = []
    names = []
    scan_started = False
    scan_started_at = None
    scan_name = ""
    started = time.monotonic()
    report.update(state="running", executed=True, run_id=owner, started_at=datetime.now(timezone.utc).isoformat(),
                  gate_passed=False, error_code=None, counts=None)
    safe_json(output, report)
    try:
        report["phase"] = "staging"
        mapping = {} if image else stage(root, workspace / "source", report["source_manifest"])
        report["phase"] = "scanner_identity"
        docker = local_docker(run)
        run([*docker, "pull", "--platform", "linux/amd64", TRIVY_IMAGE], timeout=300)
        engine = json.loads(run([*docker, "image", "inspect", "--format", "{{json .}}", TRIVY_IMAGE], timeout=30))
        require(engine.get("Os") == "linux" and engine.get("Architecture") == "amd64"
                and TRIVY_IMAGE in engine.get("RepoDigests", []), "SCANNER_IMAGE_IDENTITY_INVALID")
        report["scanner_image_id"] = engine["Id"]
        version_name = "expert-scan-" + owner[:12] + "-version"
        names.append(version_name)
        version = run([*container_args(docker, workspace, version_name, owner, network="none"),
                       TRIVY_IMAGE, "--version"], timeout=30).decode()
        require(re.search(r"(?m)^Version:\s+0\.74\.0\s*$", version) is not None, "SCANNER_VERSION_MISMATCH")
        database_name = "expert-scan-" + owner[:12] + "-db"
        names.append(database_name)
        base = container_args(docker, workspace, database_name, owner, network="bridge")
        report["phase"] = "database_download"
        run([*base, TRIVY_IMAGE, "image", "--download-db-only", "--cache-dir", "/cache", "--disable-telemetry",
             "--skip-version-check", "--no-progress", "--timeout", "5m"], timeout=360)
        report["database"] = db_attestation(workspace / "cache")
        scan_name = "expert-scan-" + owner[:12] + "-scan"
        names.append(scan_name)
        base = container_args(docker, workspace, scan_name, owner, network="none", image_scan=bool(image),
                              retain_for_diagnostics=True)
        base += mount(workspace / "raw", "/results")
        if image:
            report["phase"] = "image_export"
            inspected = json.loads(run([*docker, "image", "inspect", "--format", "{{json .}}", image], timeout=30))
            image_id = inspected.get("Id", "")
            require(re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is not None
                    and 0 < inspected.get("Size", 0) <= 32 * 1024**3, "TARGET_IMAGE_INVALID")
            report["target_image"] = {"requested_tag": image, "id": image_id,
                                      "os": identifier(inspected.get("Os")), "architecture": identifier(inspected.get("Architecture"))}
            archive = workspace / "image.tar"
            run([*docker, "image", "save", "--output", str(archive), image_id], timeout=600)
            require(archive.is_file() and 0 < archive.stat().st_size <= 40 * 1024**3, "TARGET_IMAGE_ARCHIVE_MISSING")
            report["target_image"]["archive_sha256"] = digest(archive)
            base += mount(archive, "/input/image.tar", True)
            operation = ["image", "--input", "/input/image.tar"]
        else:
            base += mount(workspace / "source", "/scan", True)
            operation = ["fs", "/scan", "--include-dev-deps"]
        report["scan_storage_telemetry"] = {"before_scan": fixed_mount_storage_bytes(
            workspace, input_archive=archive if image else None, image_scan=bool(image))}
        report["phase"] = "offline_scan"
        scan_started = True
        scan_started_at = time.monotonic()
        scan_timeout = report["scan_timeout_seconds"]
        scan_arguments = [*base, TRIVY_IMAGE, *operation, "--cache-dir", "/cache", "--skip-db-update",
             "--skip-java-db-update", "--skip-check-update", "--offline-scan", "--disable-telemetry",
             "--skip-version-check", "--no-progress", "--include-non-failures", "--list-all-pkgs", "--parallel", "2",
             "--scanners", ",".join(SCANNERS), "--severity", ",".join(SEVERITIES), "--exit-code", "0",
             "--format", "json", "--output", "/results/scan.json", "--timeout", f"{scan_timeout}s"]
        if image:
            run_scan_with_host_free_watchdog(scan_arguments, docker=docker, name=scan_name, owner=owner,
                                             workspace=workspace, run=run, timeout=scan_timeout + 60)
        else:
            run(scan_arguments, timeout=scan_timeout + 60)
        require(db_attestation(workspace / "cache") == report["database"], "DATABASE_CHANGED_DURING_SCAN")
        report["phase"] = "report_validation"
        summary = summarize(workspace / "raw/scan.json", mapping, image=bool(image))
        report.update(summary, state="passed" if summary["gate_passed"] else "findings")
    except (Exception, KeyboardInterrupt) as error:
        report.update(state="error", gate_passed=False,
                      error_code=error.code if isinstance(error, ScanError) else "SCAN_ABORTED")
        if isinstance(error, ScanError) and error.diagnostics:
            report["command_diagnostics"] = safe_command_diagnostics(error.diagnostics)
    finally:
        if scan_started_at is not None:
            report["scanner_elapsed_seconds"] = round(time.monotonic() - scan_started_at, 3)
            artifact = safe_file_presence(workspace / "raw/scan.json")
            report["scanner_result_artifact"] = artifact
            if report["state"] == "error":
                command_diagnostics = report.get("command_diagnostics", {})
                if not isinstance(command_diagnostics, dict):
                    command_diagnostics = {}
                report["failure_context"] = {"phase": report.get("phase", "unknown"),
                    "raw_report_present": artifact["present"],
                    "scanner_elapsed_seconds": report["scanner_elapsed_seconds"],
                    "command_reason": command_diagnostics.get("reason", "UNCLASSIFIED_COMMAND_FAILURE"),
                    "scanner_error_categories": command_diagnostics.get(
                        "scanner_error_categories", ["UNCLASSIFIED_COMMAND_FAILURE"])}
        if scan_started:
            try:
                telemetry = report.setdefault("scan_storage_telemetry", {})
                if isinstance(telemetry, dict):
                    telemetry["after_scan_before_cleanup"] = fixed_mount_storage_bytes(
                        workspace, input_archive=workspace / "image.tar" if image else None, image_scan=bool(image))
            except Exception:
                report["storage_telemetry_error"] = "STORAGE_TELEMETRY_FAILED"
            try:
                report["scanner_container"] = scanner_exit_metadata(docker, scan_name, owner, run)
            except Exception:
                report["scanner_container"] = {"available": False, "reason": "INSPECTION_FAILED"}
            if report["scanner_container"].get("oom_killed") is True:
                report.update(state="error", gate_passed=False, operational_failure_reason="CONTAINER_OOM_KILLED")
            elif report["state"] == "error":
                report["operational_failure_reason"] = report.get("command_diagnostics", {}).get(
                    "reason", "UNCLASSIFIED_COMMAND_FAILURE")
        for name in names:
            try:
                cleanup_container(docker, name, owner, run)
            except Exception:
                report.update(state="error", gate_passed=False, cleanup_error="OWNED_CONTAINER_CLEANUP_FAILED")
        # Only exact private outputs created under this UUID are removed. Never
        # list or delete app containers, images, volumes or arbitrary cache roots.
        try:
            cleanup_export_temporaries(root, workspace, owner)
            for path in (workspace / "raw/scan.json", workspace / "image.tar"):
                if path.is_file():
                    path.unlink()
            private_tmp = workspace / "tmp"
            if private_tmp.exists():
                require(private_tmp.resolve() == workspace.resolve() / "tmp", "TMP_CLEANUP_SCOPE_INVALID")
                shutil.rmtree(private_tmp)
            staged_source = workspace / "source"
            if staged_source.exists():
                require(staged_source.resolve() == workspace.resolve() / "source"
                        and workspace.resolve().parent == (root / ".cache/security-scans").resolve(), "STAGE_CLEANUP_SCOPE_INVALID")
                shutil.rmtree(staged_source)
        except Exception:
            report.update(state="error", gate_passed=False, cleanup_error="PRIVATE_SCAN_CLEANUP_FAILED")
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        safe_json(output, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--image")
    parser.add_argument("--scan-timeout-seconds", type=int,
        help="Explicit image-scan deadline, 1200..2400 seconds (default 1200); host bound is +60 seconds.")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        if not args.execute:
            result = plan(ROOT, args.image, scan_timeout_seconds=args.scan_timeout_seconds)
        else:
            output = args.output or ROOT / "docs/implementation/evidence/p12-scans" / (uuid4().hex + ".json")
            result = execute(ROOT, output, args.image, scan_timeout_seconds=args.scan_timeout_seconds)
        print(json.dumps({key: result.get(key) for key in ("state", "executed", "mode", "source_file_count",
            "source_manifest_sha256", "scanner_image", "counts", "high_critical_count", "gate_passed", "error_code")}, indent=2))
        return 0 if result["state"] in {"planned", "passed"} else 1
    except Exception as error:
        print(json.dumps({"state": "error", "error_code": error.code if isinstance(error, ScanError) else "SCAN_ABORTED"}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
