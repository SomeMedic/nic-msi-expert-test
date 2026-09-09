"""Private child-process entrypoint; sandbox precedes PDF and model imports."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
import sys
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _Closed(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class _Limits(_Closed):
    max_source_bytes: int = Field(ge=1, le=52_428_800, strict=True)
    max_pages: int = Field(ge=1, le=500, strict=True)
    max_artifact_bytes: int = Field(ge=1, le=268_435_456, strict=True)
    memory_bytes: int = Field(ge=256 * 1024**2, le=16 * 1024**3, strict=True)
    cpu_seconds: int = Field(ge=1, le=7200, strict=True)


class _Config(_Closed):
    max_characters: int = Field(default=10_000_000, ge=1, le=10_000_000, strict=True)
    max_blocks: int = Field(default=100_000, ge=1, le=100_000, strict=True)
    render_dpi: int = Field(default=216, ge=72, le=300, strict=True)
    symbol_render_dpi: int = Field(default=576, ge=300, le=600, strict=True)
    max_page_pixels: int = Field(default=40_000_000, ge=1, le=40_000_000, strict=True)


class _Request(_Closed):
    schema_version: int = Field(ge=1, le=1, strict=True)
    source_path: Path
    output_dir: Path
    version_id: UUID
    parse_generation_id: UUID
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_size_bytes: int = Field(ge=1, le=52_428_800, strict=True)
    title: str = Field(min_length=1, max_length=500)
    assets_path: Path | None
    asset_lock_path: Path | None = None
    review_registry_path: Path | None = None
    enforce_sandbox: bool = Field(strict=True)
    limits: _Limits
    parser_config: _Config = Field(default_factory=_Config)
    parser_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    implementation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    runtime_profile: dict | None = None
    wall_seconds: float = Field(gt=0, le=3600)


def _write_status(directory: Path, data: dict) -> None:
    # Fixed filenames in a parent-created exclusive directory. No caller path is
    # used for a result, and raw exception/vendor strings never become status.
    wire = json.dumps({"schema_version": 1, **data}, allow_nan=False).encode("utf-8")
    path = directory / "status.tmp"
    with path.open("xb") as output:
        output.write(wire)
    path.replace(directory / "status.json")


def _safe_code(error: Exception) -> str:
    allowed = {
        "PDF_INVALID", "SOURCE_UNAVAILABLE", "SIZE_LIMIT_EXCEEDED",
        "DEADLINE_EXCEEDED", "EXTRACTION_QUALITY_FAILED", "GENERATION_INVALID",
        "DEPENDENCY_UNAVAILABLE", "INTERNAL_ERROR",
    }
    code = getattr(error, "code", None)
    return code if isinstance(code, str) and code in allowed else "GENERATION_INVALID"


def execute(request_path: Path) -> int:
    directory = Path.cwd().resolve()
    try:
        if (request_path.is_symlink() or request_path.is_junction()
                or request_path.resolve().parent != directory
                or request_path.name != "request.json"
                or not 0 < request_path.stat().st_size <= 65536):
            raise ValueError("Invalid parser request file")
        request = _Request.model_validate_json(request_path.read_bytes())
        source = request.source_path.resolve()
        if request.output_dir.resolve() != directory or not request.source_path.is_absolute():
            raise ValueError("Invalid parser paths")
        if (directory.name != "output" or source.name != "original.pdf"
                or source.parent.name != "source" or source.parent.parent != directory.parent):
            raise ValueError("Invalid parser source workspace")
        if source.stat().st_size != request.source_size_bytes:
            raise ValueError("Invalid parser source size")
        if request.asset_lock_path is not None and (
            request.asset_lock_path.name != "asset-lock.json"
            or request.asset_lock_path.resolve().parent != directory
        ):
            raise ValueError("Invalid parser asset lock")
        if request.review_registry_path is not None and (
            request.review_registry_path.name != "region-reviews.json"
            or request.review_registry_path.resolve().parent != directory
            or not 0 < request.review_registry_path.stat().st_size <= 2 * 1024**2
        ):
            raise ValueError("Invalid region review registry")

        from .parser_sandbox import SandboxLimits, configure_sandbox

        sandbox = configure_sandbox(SandboxLimits(
            source_path=source,
            assets_path=request.assets_path,
            output_dir=directory,
            memory_bytes=request.limits.memory_bytes,
            cpu_seconds=request.limits.cpu_seconds,
            file_bytes=request.limits.max_artifact_bytes,
        ), enforce=request.enforce_sandbox)

        # No native PDF, OCR, torch, service settings or credential readers are
        # imported before the OS boundary is installed.
        from .dto import ParseRequest, ParserLimits, ParserRuntimeProfile, RegionReviewRegistry
        from .pipeline import parse_document
        from .runner import ParserFailure, implementation_fingerprint

        if implementation_fingerprint() != request.implementation_sha256:
            raise ParserFailure("DEPENDENCY_UNAVAILABLE")

        runtime = ParserRuntimeProfile(**asdict(sandbox))
        reviews = (RegionReviewRegistry.model_validate_json(request.review_registry_path.read_bytes()).reviews
                   if request.review_registry_path else ())
        if request.runtime_profile is not None:
            _verify_profile(request, runtime)
            profile_hash = hashlib.sha256(json.dumps(request.runtime_profile, sort_keys=True,
                separators=(",", ":"), allow_nan=False).encode()).hexdigest()
            runtime = runtime.model_copy(update={"requested_profile_sha256": profile_hash})

        parsed = parse_document(source, ParseRequest(
            version_id=request.version_id, parse_generation_id=request.parse_generation_id,
            source_sha256=request.source_sha256, title=request.title,
            region_reviews=reviews,
            limits=ParserLimits(
                max_bytes=request.limits.max_source_bytes, max_pages=request.limits.max_pages,
                max_artifact_bytes=request.limits.max_artifact_bytes,
                wall_seconds=min(480, math.ceil(request.wall_seconds)),
                **request.parser_config.model_dump(),
            ),
        ), artifacts_path=request.assets_path, asset_lock_path=request.asset_lock_path)
        manifest = parsed.document.manifest.model_copy(update={
            "parser_fingerprint": request.parser_fingerprint, "runtime_profile": runtime,
        })
        parsed = parsed.model_copy(update={"document": parsed.document.model_copy(update={"manifest": manifest})})

        digest, size = hashlib.sha256(), 0
        encoder = json.JSONEncoder(ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        with (directory / "result.json").open("xb") as output:
            for fragment in encoder.iterencode(parsed.model_dump(mode="json")):
                wire = fragment.encode("utf-8")
                size += len(wire)
                if size > request.limits.max_artifact_bytes:
                    from .runner import ParserFailure
                    raise ParserFailure("SIZE_LIMIT_EXCEEDED")
                output.write(wire)
                digest.update(wire)
        _write_status(directory, {
            "status": "succeeded", "sha256": digest.hexdigest(), "size_bytes": size,
            "sandbox_enforced": sandbox.enforced,
        })
        return 0
    except Exception as error:
        try:
            _write_status(directory, {"status": "failed", "code": _safe_code(error)})
        except OSError:
            pass
        return 1


def _verify_profile(request: _Request, runtime) -> None:
    """A requested production profile must match actual installed constraints."""
    from importlib.metadata import version
    from .runner import ParserFailure

    profile = request.runtime_profile
    assert profile is not None
    exact = {
        "python": sys.version.split()[0], "address_space_bytes": runtime.memory_bytes,
        "cpu_seconds": runtime.cpu_seconds, "wall_seconds": request.wall_seconds,
        "cpu_affinity_count": runtime.cpu_count, "child_open_files": runtime.open_files,
        "child_uid_process_and_thread_limit": runtime.processes,
        "max_source_bytes": request.limits.max_source_bytes, "max_pages": request.limits.max_pages,
        "max_artifact_bytes": request.limits.max_artifact_bytes,
        "render_dpi": request.parser_config.render_dpi,
        "symbol_render_dpi": request.parser_config.symbol_render_dpi,
        "max_page_pixels": request.parser_config.max_page_pixels,
        "seccomp_network_denied": runtime.seccomp_network_denied,
        "non_thread_process_creation_denied": runtime.process_creation_denied,
    }
    if (not runtime.enforced or not runtime.no_new_privs
            or any(profile.get(key) != value for key, value in exact.items())
            or runtime.memory_max_bytes is None
            or runtime.memory_max_bytes != profile.get("physical_memory_bytes")
            or runtime.swap_max_bytes != profile.get("swap_bytes")
            or runtime.landlock_abi < profile.get("minimum_landlock_abi", 100)):
        raise ParserFailure("DEPENDENCY_UNAVAILABLE")
    if any(version(package) != profile.get(package) for package in ("docling", "easyocr", "torch", "pymupdf")):
        raise ParserFailure("DEPENDENCY_UNAVAILABLE")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True, type=Path)
    arguments = parser.parse_args()
    return execute(arguments.request)


if __name__ == "__main__":
    sys.exit(main())
