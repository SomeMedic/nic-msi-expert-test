"""Run a credential-free PDF parser in an owned, bounded child workspace."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
from typing import AsyncIterator, Any
from uuid import UUID, uuid4


_SAFE_FAILURES = frozenset({
    "PDF_INVALID", "SOURCE_UNAVAILABLE", "SIZE_LIMIT_EXCEEDED",
    "DEADLINE_EXCEEDED", "EXTRACTION_QUALITY_FAILED", "GENERATION_INVALID",
    "DEPENDENCY_UNAVAILABLE", "INTERNAL_ERROR",
})
_STATUS_LIMIT = 4096


def implementation_fingerprint() -> str:
    """Bind source installed in each process without exposing its host path."""
    digest = hashlib.sha256()
    directory = Path(__file__).resolve().parent
    for path in sorted(directory.glob("*.py")):
        data = path.read_bytes()
        if len(data) > 8 * 1024**2:
            raise ValueError("Parser implementation file exceeds its bound")
        digest.update(path.name.encode("utf-8") + b"\0")
        digest.update(hashlib.sha256(data).digest())
    return digest.hexdigest()


class ParserFailure(Exception):
    """Stable code only; child output and exception text never cross this boundary."""

    def __init__(self, code: str):
        self.code = code if code in _SAFE_FAILURES else "INTERNAL_ERROR"
        super().__init__(self.code)


@dataclass(frozen=True)
class RunnerOptions:
    python: Path
    root: Path
    assets_path: Path | None = None
    asset_lock_path: Path | None = None
    runtime_profile_path: Path | None = None
    review_registry_path: Path | None = None
    module_paths: tuple[Path, ...] = ()
    timeout_seconds: float = 480
    terminate_grace_seconds: float = 2
    max_source_bytes: int = 52_428_800
    max_artifact_bytes: int = 268_435_456
    # Virtual address space; measured physical cgroup limit is 6 GiB, no swap.
    memory_bytes: int = 8 * 1024**3
    cpu_seconds: int = 960
    max_pages: int = 500
    enforce_sandbox: bool = True

    def __post_init__(self) -> None:
        if (not 0 < self.timeout_seconds <= 3600
                or not 0 < self.terminate_grace_seconds <= 10
                or not 1 <= self.max_source_bytes <= 52_428_800
                or not 1 <= self.max_artifact_bytes <= 268_435_456
                or not 256 * 1024**2 <= self.memory_bytes <= 16 * 1024**3
                or not 1 <= self.cpu_seconds <= 7200
                or not 1 <= self.max_pages <= 500):
            raise ValueError("Invalid parser runner limits")
        if not self.python.is_absolute() or not self.python.is_file():
            raise ValueError("Parser Python must be an existing absolute executable")
        if any(not p.is_absolute() or not p.is_dir() for p in self.module_paths):
            raise ValueError("Parser module paths must be explicit existing directories")
        if self.assets_path is not None and (
            not self.assets_path.is_absolute() or not self.assets_path.is_dir()
        ):
            raise ValueError("Parser assets must be an explicit existing directory")
        if self.asset_lock_path is not None and (
            not self.asset_lock_path.is_absolute() or not self.asset_lock_path.is_file()
            or self.asset_lock_path.stat().st_size > 2 * 1024**2
        ):
            raise ValueError("Parser asset lock must be an explicit bounded file")
        if self.runtime_profile_path is not None and (
            not self.runtime_profile_path.is_absolute() or not self.runtime_profile_path.is_file()
            or self.runtime_profile_path.stat().st_size > 16384
        ):
            raise ValueError("Parser runtime profile must be an explicit bounded file")
        if self.review_registry_path is not None and (
            not self.review_registry_path.is_absolute() or not self.review_registry_path.is_file()
            or self.review_registry_path.stat().st_size > 2 * 1024**2
        ):
            raise ValueError("Region review registry must be an explicit bounded file")


@dataclass(frozen=True)
class ParserOutput:
    path: Path
    sha256: str
    size_bytes: int
    sandbox_enforced: bool


def _linked(path: Path) -> bool:
    return path.is_symlink() or path.is_junction()


def _checksum(path: Path, maximum: int) -> tuple[str, int]:
    digest, count = hashlib.sha256(), 0
    if _linked(path) or not path.is_file():
        raise ParserFailure("SOURCE_UNAVAILABLE")
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            count += len(chunk)
            if count > maximum:
                raise ParserFailure("SIZE_LIMIT_EXCEEDED")
            digest.update(chunk)
    return digest.hexdigest(), count


def _load_status(directory: Path) -> dict[str, Any]:
    path = directory / "status.json"
    if (_linked(path) or not path.is_file() or not 0 < path.stat().st_size <= _STATUS_LIMIT):
        raise ParserFailure("GENERATION_INVALID")
    try:
        status = json.loads(path.read_bytes())
    except (OSError, ValueError):
        raise ParserFailure("GENERATION_INVALID") from None
    if not isinstance(status, dict) or status.get("schema_version") != 1:
        raise ParserFailure("GENERATION_INVALID")
    return status


async def _finish(awaitable: Any) -> Any:
    """Do not orphan a spawn/kill/wait operation when cancellation repeats."""
    task = asyncio.ensure_future(awaitable)
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


async def _hash(path: Path, maximum: int) -> tuple[str, int]:
    task = asyncio.create_task(asyncio.to_thread(_checksum, path, maximum))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # The original may be deleted by its owner's outer context immediately
        # after cancellation. Finish this bounded reader before releasing it.
        await _finish(task)
        raise


class ParserRunner:
    def __init__(self, options: RunnerOptions):
        self.options = options
        options.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if _linked(options.root):
            raise ValueError("Parser workspace root must not be a link")
        self.root = options.root.resolve(strict=True)
        self._implementation = implementation_fingerprint()
        # Freeze the exact recipe inputs at startup. A changed lock cannot give
        # one generation a second identity halfway through parsing or replay.
        self._asset_lock = options.asset_lock_path.read_bytes() if options.asset_lock_path else None
        self._review_registry = options.review_registry_path.read_bytes() if options.review_registry_path else None
        from .dto import RegionReviewRegistry
        self.trusted_region_reviews = (RegionReviewRegistry.model_validate_json(self._review_registry).reviews
                                       if self._review_registry else ())
        self.runtime_profile = (json.loads(options.runtime_profile_path.read_bytes())
                                if options.runtime_profile_path else None)
        if self.runtime_profile is not None and not isinstance(self.runtime_profile, dict):
            raise ValueError("Invalid parser runtime profile")

    def fingerprint(self, recipe: str = "p04-parser-v1", *, normalizer_version: str = "mapped-nfc-v1",
                    structure_version: str = "generic-tree-v1", parser_config: dict[str, Any] | None = None) -> str:
        value = {
            "schema_version": "p04.recipe.v1", "recipe": recipe,
            "implementation_sha256": self._implementation,
            "normalizer_version": normalizer_version, "structure_version": structure_version,
            "runtime_profile": self.runtime_profile,
            "asset_lock_sha256": hashlib.sha256(self._asset_lock).hexdigest() if self._asset_lock else None,
            "review_registry_sha256": hashlib.sha256(self._review_registry).hexdigest() if self._review_registry else None,
            "limits": {name: getattr(self.options, name) for name in (
                "timeout_seconds", "max_source_bytes", "max_artifact_bytes", "memory_bytes",
                "cpu_seconds", "max_pages", "enforce_sandbox",
            )},
            "parser_config": {"max_characters": 10_000_000, "max_blocks": 100_000,
                              "render_dpi": 216, "symbol_render_dpi": 576,
                              "max_page_pixels": 40_000_000, **(parser_config or {})},
        }
        return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                         allow_nan=False, separators=(",", ":")).encode()).hexdigest()

    def _environment(self, directory: Path) -> dict[str, str]:
        # Nothing from service settings, proxy variables, tokens, cloud SDK env or
        # the parent's Python import settings is inherited.
        result = {
            "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1", "PYTHONUTF8": "1",
            "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
            "HF_HUB_DISABLE_TELEMETRY": "1", "DO_NOT_TRACK": "1",
            "TOKENIZERS_PARALLELISM": "false", "OMP_NUM_THREADS": "2",
            "MKL_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2",
            "TMPDIR": str(directory / "tmp"), "TMP": str(directory / "tmp"),
            "TEMP": str(directory / "tmp"), "HOME": str(directory / "tmp"),
            "HF_HOME": str(directory / "tmp" / "huggingface"),
            "XDG_CACHE_HOME": str(directory / "tmp" / "cache"),
        }
        if sys.platform == "win32":
            for name in ("SYSTEMROOT", "WINDIR"):
                if os.environ.get(name):
                    result[name] = os.environ[name]
        if self.options.module_paths:
            result["PYTHONPATH"] = os.pathsep.join(str(p) for p in self.options.module_paths)
        return result

    def _cleanup(self, directory: Path, token: str) -> None:
        # Only this invocation's random workspace may be recursively removed.
        # shutil.rmtree does not traverse directory symlinks/Windows junctions.
        if (_linked(directory) or directory.parent.resolve() != self.root
                or directory.resolve().parent != self.root
                or not re.fullmatch(r"parser-[a-z0-9_]{8}", directory.name)):
            raise ParserFailure("INTERNAL_ERROR")
        marker = directory / "owner.json"
        if (_linked(marker) or not marker.is_file() or marker.stat().st_size > 256
                or marker.read_text(encoding="utf-8") != token):
            raise ParserFailure("INTERNAL_ERROR")
        shutil.rmtree(directory)

    async def _wait(self, process: subprocess.Popen[bytes]) -> int:
        # Windows psycopg uses a selector loop, which has no asyncio subprocess
        # transport. Popen plus bounded polling supports the same worker loop on
        # both platforms without changing process-global event-loop policy.
        while process.poll() is None:
            await asyncio.sleep(.025)
        return process.returncode

    async def _stop(self, process: subprocess.Popen[bytes]) -> None:
        def terminate_group(sig: int) -> None:
            try:
                if sys.platform != "win32":
                    os.killpg(process.pid, sig)
                elif sig == signal.SIGTERM:
                    process.terminate()
                else:
                    process.kill()
            except ProcessLookupError:
                pass

        if process.poll() is None:
            terminate_group(signal.SIGTERM)
            try:
                await asyncio.wait_for(self._wait(process), self.options.terminate_grace_seconds)
            except TimeoutError:
                terminate_group(getattr(signal, "SIGKILL", 9) if sys.platform != "win32" else signal.SIGTERM)
                if sys.platform == "win32" and process.poll() is None:
                    process.kill()
        await self._wait(process)
        if sys.platform != "win32":
            # Also reap a descendant left after the leader exited. Production
            # sandbox restricts process creation; this is a second cleanup fence.
            terminate_group(getattr(signal, "SIGKILL", 9))

    @asynccontextmanager
    async def run(self, source: Path, *, version_id: UUID, parse_generation_id: UUID,
                  source_sha256: str, source_size_bytes: int, title: str,
                  parser_config: dict[str, Any] | None = None, parser_recipe: str = "p04-parser-v1",
                  normalizer_version: str = "mapped-nfc-v1",
                  structure_version: str = "generic-tree-v1") -> AsyncIterator[ParserOutput]:
        """Yield verified bytes on disk; ownership lasts until the caller exits."""
        if (not re.fullmatch(r"[0-9a-f]{64}", source_sha256)
                or not 0 < source_size_bytes <= self.options.max_source_bytes
                or not title.strip() or len(title) > 500):
            raise ParserFailure("SOURCE_UNAVAILABLE")
        if _linked(source):
            raise ParserFailure("SOURCE_UNAVAILABLE")
        original = source.resolve(strict=True)
        before = await _hash(original, self.options.max_source_bytes)
        if before != (source_sha256, source_size_bytes):
            raise ParserFailure("SOURCE_UNAVAILABLE")
        directory = Path(tempfile.mkdtemp(prefix="parser-", dir=self.root))
        token = str(uuid4())
        (directory / "owner.json").write_text(token, encoding="utf-8")
        source_directory = directory / "source"
        output_directory = directory / "output"
        staged_source = source_directory / "original.pdf"
        staged_before: tuple[str, int] | None = None
        process: subprocess.Popen[bytes] | None = None
        spawn: asyncio.Task[subprocess.Popen[bytes]] | None = None
        try:
            source_directory.mkdir(mode=0o700)
            output_directory.mkdir(mode=0o700)
            (output_directory / "tmp").mkdir(mode=0o700)
            try:
                shutil.copyfile(original, staged_source)
            except OSError:
                raise ParserFailure("SOURCE_UNAVAILABLE") from None
            staged_before = await _hash(staged_source, self.options.max_source_bytes)
            if staged_before != before:
                raise ParserFailure("SOURCE_UNAVAILABLE")
            request = {
                "schema_version": 1, "source_path": str(staged_source), "output_dir": str(output_directory),
                "version_id": str(version_id), "parse_generation_id": str(parse_generation_id),
                "source_sha256": source_sha256, "source_size_bytes": source_size_bytes,
                "title": title, "assets_path": str(self.options.assets_path) if self.options.assets_path else None,
                "asset_lock_path": None,
                "review_registry_path": None,
                "enforce_sandbox": self.options.enforce_sandbox,
                "limits": {
                    "max_source_bytes": self.options.max_source_bytes, "max_pages": self.options.max_pages,
                    "max_artifact_bytes": self.options.max_artifact_bytes,
                    "memory_bytes": self.options.memory_bytes, "cpu_seconds": self.options.cpu_seconds,
                },
                "parser_config": parser_config or {},
                "parser_fingerprint": self.fingerprint(parser_recipe, normalizer_version=normalizer_version,
                    structure_version=structure_version, parser_config=parser_config),
                "runtime_profile": self.runtime_profile,
                "implementation_sha256": self._implementation,
                "wall_seconds": self.options.timeout_seconds,
            }
            if self._asset_lock is not None:
                lock_copy = output_directory / "asset-lock.json"
                lock_copy.write_bytes(self._asset_lock)
                request["asset_lock_path"] = str(lock_copy)
            if self._review_registry is not None:
                review_copy = output_directory / "region-reviews.json"
                review_copy.write_bytes(self._review_registry)
                request["review_registry_path"] = str(review_copy)
            wire = json.dumps(request, ensure_ascii=False, allow_nan=False).encode("utf-8")
            if len(wire) > 65536:
                raise ParserFailure("SIZE_LIMIT_EXCEEDED")
            request_path = output_directory / "request.json"
            request_path.write_bytes(wire)
            spawn = asyncio.create_task(asyncio.to_thread(
                subprocess.Popen,
                [str(self.options.python), "-m", "expert_ingest.parsing.parser_cli",
                 "--request", str(request_path)], cwd=output_directory, env=self._environment(output_directory),
                # No vendor output is retained or inherited by service logs.
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, close_fds=True,
                start_new_session=sys.platform != "win32",
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if sys.platform == "win32" else 0,
            ))
            process = await asyncio.shield(spawn)
            try:
                await asyncio.wait_for(self._wait(process), self.options.timeout_seconds)
            except TimeoutError:
                raise ParserFailure("DEADLINE_EXCEEDED") from None
            status = _load_status(output_directory)
            if status.get("status") == "failed":
                code = status.get("code")
                raise ParserFailure(code if isinstance(code, str) else "INTERNAL_ERROR")
            if (process.returncode != 0 or status.get("status") != "succeeded"
                    or set(status) != {"schema_version", "status", "sha256", "size_bytes", "sandbox_enforced"}
                    or type(status.get("sandbox_enforced")) is not bool
                    or self.options.enforce_sandbox and not status["sandbox_enforced"]):
                raise ParserFailure("GENERATION_INVALID")
            output = output_directory / "result.json"
            digest, size = await _hash(output, self.options.max_artifact_bytes)
            if (size == 0 or type(status.get("size_bytes")) is not int
                    or status.get("sha256") != digest or status.get("size_bytes") != size):
                raise ParserFailure("GENERATION_INVALID")
            staged_after = await _hash(staged_source, self.options.max_source_bytes)
            after = await _hash(original, self.options.max_source_bytes)
            if staged_before != staged_after or before != after:
                raise ParserFailure("SOURCE_UNAVAILABLE")
            yield ParserOutput(output, digest, size, status["sandbox_enforced"])
        finally:
            # A cancelled spawn may already have created the child. Retrieve and
            # stop it before releasing the source/workspace, even on repeat cancel.
            if process is None and spawn is not None:
                try:
                    process = await _finish(spawn)
                except (OSError, RuntimeError):
                    pass
            if process is not None:
                await _finish(self._stop(process))
            self._cleanup(directory, token)
