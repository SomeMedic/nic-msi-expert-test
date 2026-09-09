"""Download an immutable PG-resolved original into a bounded, owned workspace."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
from typing import Any, BinaryIO, Protocol
from uuid import UUID


class SourceFailure(Exception):
    """Only these fixed messages may become durable job errors."""

    def __init__(self, code: str, safe_message: str, *, retryable: bool = False):
        super().__init__(safe_message)
        self.code = code
        self.safe_message = safe_message
        self.retryable = retryable


@dataclass(frozen=True)
class SourceReference:
    job_id: UUID
    document_id: UUID
    version_id: UUID
    object_id: UUID
    bucket: str
    key: str
    object_version_id: str | None
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class LocalSource:
    reference: SourceReference
    path: Path


class ObjectStorage(Protocol):
    """The client must have bounded connect/read timeouts and no unbounded retries."""

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


def _close_body_preserving_primary(body: Any, *, primary_failure: bool) -> None:
    try:
        body.close()
    except Exception:
        if not primary_failure:
            raise SourceFailure("SOURCE_UNAVAILABLE", "Original source stream could not close",
                                retryable=True) from None


def _lock(handle: BinaryIO) -> None:
    # A kernel lock also protects a long-running worker after its PG lease expires.
    handle.seek(0)
    if sys.platform == "win32":
        import msvcrt
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _linked(path: Path) -> bool:
    return path.is_symlink() or path.is_junction()


class OriginalSource:
    def __init__(self, storage: ObjectStorage, root: Path, *, bucket: str = "originals",
                 max_bytes: int = 52_428_800, timeout_seconds: float = 60):
        if max_bytes < 1 or not 0 < timeout_seconds <= 600:
            raise ValueError("Invalid original download limits")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if _linked(root):
            raise ValueError("Temporary root must not be a link")
        self.root = root.resolve(strict=True)
        self.storage = storage
        self.bucket = bucket
        self.max_bytes = max_bytes
        self.timeout_seconds = timeout_seconds

    def _validate(self, reference: SourceReference) -> None:
        expected = f"originals/{reference.document_id}/{reference.version_id}/{reference.sha256}.pdf"
        if (reference.bucket != self.bucket or reference.key != expected
                or not re.fullmatch(r"[0-9a-f]{64}", reference.sha256)
                or not 0 < reference.size_bytes <= self.max_bytes):
            raise SourceFailure("SOURCE_UNAVAILABLE", "Original source identity is invalid")

    def _cleanup(self, directory: Path) -> bool:
        """Remove only the three files owned here; never recursively delete a tree."""
        if (_linked(directory) or directory.parent.resolve() != self.root
                or directory.resolve().parent != self.root):
            return False
        allowed = {"original.pdf", "owner.json", "lease.lock"}
        entries = list(directory.iterdir())
        if any(p.name not in allowed or _linked(p) or not p.is_file() for p in entries):
            return False
        for path in entries:
            path.unlink()
        directory.rmdir()
        return True

    def janitor(self, *, older_than_seconds: float = 3600, limit: int = 100) -> int:
        """Skip occupied, linked, foreign, recent, or modified directories."""
        if older_than_seconds < self.timeout_seconds or not 1 <= limit <= 1000:
            raise ValueError("Invalid janitor limits")
        removed = 0
        examined = 0
        for directory in self.root.iterdir():
            if examined >= limit:
                break
            examined += 1
            if not re.fullmatch(r"ingestion-[a-z0-9_]{8}", directory.name):
                continue
            if _linked(directory) or not directory.is_dir():
                continue
            marker, lockfile = directory / "owner.json", directory / "lease.lock"
            try:
                if (_linked(marker) or _linked(lockfile) or marker.stat().st_size > 1024
                        or time.time() - marker.stat().st_mtime < older_than_seconds):
                    continue
                with lockfile.open("r+b") as handle:
                    _lock(handle)
                    data = json.loads(marker.read_text(encoding="utf-8"))
                    if not isinstance(data, dict) or data.get("kind") != "expert-ingestion-original-v1":
                        continue
                    UUID(data["job_id"])
                    UUID(data["owner"])
                    if type(data["epoch"]) is not int or data["epoch"] < 1:
                        continue
                # The directory cannot be reused by another job: random exclusive creation.
                removed += self._cleanup(directory)
            except (OSError, ValueError, KeyError, TypeError):
                continue
        return removed

    def _download(self, reference: SourceReference, target: Path, stop: threading.Event) -> None:
        started = time.monotonic()

        def check() -> None:
            if stop.is_set():
                raise SourceFailure("DEADLINE_EXCEEDED", "Original download was interrupted", retryable=True)
            if time.monotonic() - started >= self.timeout_seconds:
                raise SourceFailure("DEADLINE_EXCEEDED", "Original download timed out", retryable=True)

        response: dict[str, Any] | None = None
        primary_failure = False
        try:
            check()
            request: dict[str, Any] = {"Bucket": reference.bucket, "Key": reference.key}
            if reference.object_version_id is not None:
                request["VersionId"] = reference.object_version_id
            response = self.storage.get_object(**request)
            check()
            if (response.get("ContentLength") != reference.size_bytes
                    or (reference.object_version_id is not None
                        and response.get("VersionId") != reference.object_version_id)):
                raise SourceFailure("SOURCE_UNAVAILABLE", "Original source identity does not match")
            digest = hashlib.sha256()
            size = 0
            prefix = b""
            with target.open("xb") as output:
                while True:
                    check()
                    chunk = response["Body"].read(min(262_144, self.max_bytes - size + 1))
                    check()
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > min(self.max_bytes, reference.size_bytes):
                        raise SourceFailure("SIZE_LIMIT_EXCEEDED", "Original source exceeds its declared size")
                    if len(prefix) < 5:
                        prefix = (prefix + chunk)[:5]
                    digest.update(chunk)
                    output.write(chunk)
            if size != reference.size_bytes or digest.hexdigest() != reference.sha256:
                raise SourceFailure("SOURCE_UNAVAILABLE", "Original source checksum does not match")
            if prefix != b"%PDF-":
                raise SourceFailure("PDF_INVALID", "Original source is not a PDF")
        except SourceFailure:
            primary_failure = True
            raise
        except Exception:
            # SDK error text may contain credentials, object keys, or server responses.
            primary_failure = True
            raise SourceFailure("SOURCE_UNAVAILABLE", "Original source download failed", retryable=True) from None
        finally:
            if response is not None and response.get("Body") is not None:
                _close_body_preserving_primary(response["Body"], primary_failure=primary_failure)

    @asynccontextmanager
    async def open(self, reference: SourceReference, owner: UUID, epoch: int):
        self._validate(reference)
        if epoch < 1:
            raise ValueError("Invalid ingestion epoch")
        directory = Path(tempfile.mkdtemp(prefix="ingestion-", dir=self.root))
        stop = threading.Event()
        handle: BinaryIO | None = None
        try:
            handle = (directory / "lease.lock").open("x+b")
            handle.write(b"1")
            handle.flush()
            _lock(handle)
            (directory / "owner.json").write_text(json.dumps({
                "kind": "expert-ingestion-original-v1", "job_id": str(reference.job_id),
                "owner": str(owner), "epoch": epoch,
            }), encoding="utf-8")
            target = directory / "original.pdf"
            download = asyncio.create_task(asyncio.to_thread(self._download, reference, target, stop))
            try:
                await asyncio.shield(download)
            except asyncio.CancelledError:
                stop.set()
                # Cancelling to_thread does not stop its OS thread. Join before cleanup.
                while not download.done():
                    try:
                        await asyncio.shield(download)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
                if download.done() and not download.cancelled():
                    download.exception()
                raise
            yield LocalSource(reference, target)
        finally:
            stop.set()
            if handle is not None:
                handle.close()
            self._cleanup(directory)
