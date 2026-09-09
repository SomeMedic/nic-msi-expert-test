"""Bound Starlette's streaming multipart parser without a second PDF copy."""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
import re

from fastapi import Request
from pydantic import ValidationError
from python_multipart.exceptions import MultipartParseError
from python_multipart.multipart import parse_options_header
from starlette.datastructures import UploadFile
from starlette.formparsers import MultiPartException, MultiPartParser

from expert_clients.settings import Settings
from expert_contracts.documents import VersionUploadOptions
from expert_contracts.errors import ErrorCode
from expert_api.errors import ApiError

HEADER_LIMIT = 16_384
CHUNK_SIZE = 65_536
_FILENAME = re.compile(r"[^\x00-\x1f\x7f/\\:]{1,500}\.pdf\Z", re.IGNORECASE)


@dataclass(frozen=True)
class ParsedUpload:
    file: UploadFile
    options: VersionUploadOptions
    sha256: str
    size: int
    filename: str


class BoundedMultipartParser(MultiPartParser):
    """The pinned Starlette parser owns spooling; callbacks add missing bounds."""

    def __init__(self, headers, stream, settings: Settings):
        super().__init__(headers, stream, max_files=1, max_fields=1,
                         max_part_size=settings.upload_metadata_max_bytes)
        self.file_limit = settings.upload_max_bytes
        self.header_bytes = 0
        self.file_bytes = 0
        self.finished = False

    def on_part_begin(self) -> None:
        self.header_bytes = 0
        self.file_bytes = 0
        super().on_part_begin()

    def _count_header(self, length: int) -> None:
        self.header_bytes += length
        if self.header_bytes > HEADER_LIMIT:
            raise ApiError(ErrorCode.INVALID_REQUEST)

    def on_header_field(self, data: bytes, start: int, end: int) -> None:
        self._count_header(end - start)
        super().on_header_field(data, start, end)

    def on_header_value(self, data: bytes, start: int, end: int) -> None:
        self._count_header(end - start)
        super().on_header_value(data, start, end)

    def on_headers_finished(self) -> None:
        names = [name for name, _value in self._current_part.item_headers]
        if len(names) != len(set(names)) or b"content-transfer-encoding" in names:
            raise ApiError(ErrorCode.INVALID_REQUEST)
        disposition = self._current_part.content_disposition or b""
        # python-multipart normalizes legacy browser Windows paths. Reject the
        # original header before that normalization conceals invalid metadata.
        if b"\\" in disposition or any(byte < 32 or byte == 127 for byte in disposition):
            raise ApiError(ErrorCode.INVALID_REQUEST)
        try:
            for value in parse_options_header(disposition)[1].values():
                value.decode("utf-8", errors="strict")
        except UnicodeError:
            raise ApiError(ErrorCode.INVALID_REQUEST) from None
        super().on_headers_finished()

    def on_part_end(self) -> None:
        if self._current_part.file is None:
            try:
                self._current_part.data.decode("utf-8", errors="strict")
            except UnicodeError:
                raise ApiError(ErrorCode.INVALID_REQUEST) from None
        super().on_part_end()

    def on_part_data(self, data: bytes, start: int, end: int) -> None:
        if self._current_part.file is not None:
            self.file_bytes += end - start
            if self.file_bytes > self.file_limit:
                raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": self.file_limit})
        super().on_part_data(data, start, end)

    def on_end(self) -> None:
        self.finished = True
        super().on_end()

    def close_files(self) -> None:
        # Includes an unfinished part, which FormData cannot see after EOF.
        for file in self._files_to_close_on_error:
            file.close()


async def _bounded_stream(request: Request, maximum: int):
    received = 0
    async for chunk in request.stream():
        received += len(chunk)
        if received > maximum:
            raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": maximum})
        # An ASGI server may supply a large chunk. Bound parser callback buffers.
        for offset in range(0, len(chunk), CHUNK_SIZE):
            yield chunk[offset:offset + CHUNK_SIZE]


def _check_headers(request: Request, settings: Settings) -> None:
    if sum(len(key) + len(value) for key, value in request.headers.raw) > HEADER_LIMIT:
        raise ApiError(ErrorCode.INVALID_REQUEST)
    for name in ("content-type", "content-length", "content-encoding"):
        if len(request.headers.getlist(name)) > 1:
            raise ApiError(ErrorCode.INVALID_REQUEST)
    if request.headers.get("content-encoding", "identity").lower() != "identity":
        raise ApiError(ErrorCode.UNSUPPORTED_MEDIA_TYPE)
    length = request.headers.get("content-length")
    if length is not None:
        if not length.isascii() or not length.isdigit():
            raise ApiError(ErrorCode.INVALID_REQUEST)
        if len(length) > 10 or int(length) > settings.upload_body_max_bytes:
            raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": settings.upload_body_max_bytes})
    media_type, parameters = parse_options_header(request.headers.get("content-type", ""))
    boundary = parameters.get(b"boundary", b"")
    if media_type != b"multipart/form-data":
        raise ApiError(ErrorCode.UNSUPPORTED_MEDIA_TYPE)
    if not 1 <= len(boundary) <= 70 or any(byte < 32 or byte > 126 for byte in boundary):
        raise ApiError(ErrorCode.INVALID_REQUEST)
    if parameters.get(b"charset", b"utf-8").lower() != b"utf-8":
        raise ApiError(ErrorCode.INVALID_REQUEST)


@asynccontextmanager
async def parse_upload(request: Request, settings: Settings):
    _check_headers(request, settings)
    parser = BoundedMultipartParser(request.headers, _bounded_stream(request, settings.upload_body_max_bytes), settings)
    try:
        try:
            form = await parser.parse()
        except (MultiPartException, MultipartParseError, UnicodeError, ValueError):
            raise ApiError(ErrorCode.INVALID_REQUEST) from None
        items = form.multi_items()
        if not parser.finished or len(items) != 2 or {key for key, _value in items} != {"file", "options"}:
            raise ApiError(ErrorCode.INVALID_REQUEST)
        file, options = form["file"], form["options"]
        if not isinstance(file, UploadFile) or not isinstance(options, str):
            raise ApiError(ErrorCode.INVALID_REQUEST)
        filename = file.filename or ""
        if _FILENAME.fullmatch(filename) is None or len(filename) > 500 or filename.strip() != filename:
            raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "file", "reason": "filename"})
        if parse_options_header(file.content_type or "")[0] != b"application/pdf":
            raise ApiError(ErrorCode.UNSUPPORTED_MEDIA_TYPE)
        if len(options.encode("utf-8")) > settings.upload_metadata_max_bytes:
            raise ApiError(ErrorCode.INVALID_REQUEST)
        try:
            parsed_options = VersionUploadOptions.model_validate_json(options)
        except ValidationError:
            raise ApiError(ErrorCode.VALIDATION_ERROR, details={"field": "options"}) from None
        digest, size = hashlib.sha256(), 0
        while chunk := await file.read(CHUNK_SIZE):
            if size == 0 and not chunk.startswith(b"%PDF-"):
                raise ApiError(ErrorCode.PDF_INVALID)
            size += len(chunk)
            if size > settings.upload_max_bytes:
                raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": settings.upload_max_bytes})
            digest.update(chunk)
        if size < 8:
            raise ApiError(ErrorCode.PDF_INVALID)
        await file.seek(0)
        yield ParsedUpload(file, parsed_options, digest.hexdigest(), size, filename)
    finally:
        parser.close_files()
