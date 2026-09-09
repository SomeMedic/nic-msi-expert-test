"""Real service-role S3/Redis fixtures with exact, disposable resource ownership.

Import ``p03_resources`` into the requesting integration test module. This module
does not modify shared conftest, infrastructure, policies, or production settings.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import partial
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID, uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import SecretStr
from expert_contracts.debug_capture import DebugObjectRef
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import ResponseError
import pytest
import pytest_asyncio


ROOT = Path(__file__).resolve().parents[2]
SECRETS = ROOT / ".secrets/local"
EVIDENCE = ROOT / "docs/implementation/evidence/p03-fixture"
S3_ENDPOINT = "http://127.0.0.1:59000"
BUCKETS = frozenset({"originals", "artifacts", "debug"})
ORIGINAL_KEY = re.compile(r"originals/([0-9a-f-]{36})/([0-9a-f-]{36})/([0-9a-f]{64})\.pdf\Z")
PARSE_ARTIFACT_KEY = re.compile(
    r"parses/([0-9a-f-]{36})/([0-9a-f-]{36})/([0-9a-f-]{36})/([0-9a-f]{64})\.(json|png)\Z"
)


class ResourceFixtureError(RuntimeError):
    """Only a safe reason code is exposed, never a provider message or secret."""


def _secret(name: str) -> SecretStr:
    __tracebackhide__ = True
    try:
        path = SECRETS / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 16_384:
            raise ValueError()
        value = path.read_text(encoding="utf-8").strip()
        if not value:
            raise ValueError()
        return SecretStr(value)
    except Exception:
        raise ResourceFixtureError("P03_RESOURCE_SECRET_UNAVAILABLE") from None


def _s3_client(access: SecretStr, secret: SecretStr):
    __tracebackhide__ = True
    client = boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, region_name="us-east-1",
        aws_access_key_id=access.get_secret_value(), aws_secret_access_key=secret.get_secret_value(),
        config=Config(connect_timeout=3, read_timeout=3, retries={"max_attempts": 0},
                      proxies={}, s3={"addressing_style": "path"}),
    )

    def fixed_endpoint(request, **kwargs):
        parsed = urlsplit(request.url)
        if (parsed.scheme, parsed.hostname, parsed.port) != ("http", "127.0.0.1", 59000):
            raise ResourceFixtureError("P03_S3_ENDPOINT_ESCAPE_BLOCKED")

    client.meta.events.register("before-send.s3", fixed_endpoint)
    return client


def _redis_credentials(role: str) -> tuple[SecretStr, dict]:
    __tracebackhide__ = True
    try:
        parsed = urlsplit(_secret(f"{role}_redis_url").get_secret_value())
        if (parsed.scheme != "redis" or parsed.hostname != "redis" or parsed.port != 6379
                or parsed.path != "/0" or parsed.username != "expert_" + role
                or not parsed.password or parsed.query or parsed.fragment):
            raise ValueError()
        password = unquote(parsed.password)
        url = SecretStr(f"redis://expert_{role}:{quote(password, safe='')}@127.0.0.1:56379/0")
        return url, {"host": "127.0.0.1", "port": 56379, "db": 0, "username": "expert_" + role,
                     "password": password, "decode_responses": True, "socket_connect_timeout": 3,
                     "socket_timeout": 3, "retry": Retry(NoBackoff(), 0), "protocol": 2,
                     "max_connections": 4}
    except Exception:
        raise ResourceFixtureError("P03_REDIS_SOURCE_CONTRACT_INVALID") from None


class ScopedRedis(Redis):
    """Actual Redis client and ACL identity; rejects foreign keys before network I/O."""

    def __init__(self, *, stream: str, owner_key: str, group: str,
                 owned_streams: set[str], notifications: dict, **kwargs):
        super().__init__(**kwargs)
        self._owner_key = owner_key
        self._owned_streams = owned_streams
        self._notifications = notifications
        self._owned_stream = stream
        self._owned_group = group

    def pipeline(self, *args, **kwargs):
        raise ResourceFixtureError("P03_REDIS_PIPELINE_BLOCKED")

    async def execute_command(self, *args, **options):
        __tracebackhide__ = True
        def text_arg(value):
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)

        command = text_arg(args[0]).upper()
        if command in {"PING", "CLIENT SETNAME", "CLIENT SETINFO"}:
            return await super().execute_command(*args, **options)
        keys = []
        if command in {"GET", "SET", "EXPIRE", "XADD", "XLEN", "XRANGE", "XREVRANGE", "XACK", "XPENDING", "XAUTOCLAIM"}:
            keys = [args[1]]
        elif command == "DEL":
            keys = list(args[1:])
        elif command in {"XGROUP CREATE", "XINFO GROUPS"}:
            keys = [args[1]]
            if command == "XGROUP CREATE" and text_arg(args[2]) != self._owned_group:
                raise ResourceFixtureError("P03_REDIS_GROUP_ESCAPE_BLOCKED")
        elif command in {"XREAD", "XREADGROUP"}:
            tokens = [text_arg(value).upper() for value in args]
            if "STREAMS" not in tokens:
                raise ResourceFixtureError("P03_REDIS_COMMAND_BLOCKED")
            offset = tokens.index("STREAMS") + 1
            remaining = args[offset:]
            keys = list(remaining[:len(remaining) // 2])
            if command == "XREADGROUP" and (tokens[1] != "GROUP" or text_arg(args[2]) != self._owned_group):
                raise ResourceFixtureError("P03_REDIS_GROUP_ESCAPE_BLOCKED")
        else:
            raise ResourceFixtureError("P03_REDIS_COMMAND_BLOCKED")
        keys = [text_arg(key) for key in keys]
        if not keys or any(key not in self._owned_streams and key != self._owner_key for key in keys):
            raise ResourceFixtureError("P03_REDIS_KEY_ESCAPE_BLOCKED")
        if command.startswith("X") and any(key not in self._owned_streams for key in keys):
            raise ResourceFixtureError("P03_REDIS_STREAM_ESCAPE_BLOCKED")
        if command in {"XGROUP CREATE", "XREADGROUP", "XACK", "XPENDING", "XAUTOCLAIM"} and keys != [self._owned_stream]:
            raise ResourceFixtureError("P03_REDIS_GROUP_STREAM_ESCAPE_BLOCKED")
        if command in {"XACK", "XPENDING", "XAUTOCLAIM"} and text_arg(args[2]) != self._owned_group:
            raise ResourceFixtureError("P03_REDIS_GROUP_ESCAPE_BLOCKED")
        record = self._notifications.get(keys[0]) if command == "XADD" else None
        if record is not None:
            if not record["preexisting_absent"]:
                raise ResourceFixtureError("P03_REDIS_UNOWNED_WRITE_BLOCKED")
            record["attempted"] = True
        result = await super().execute_command(*args, **options)
        if record is not None:
            record["created"] = True
        return result


@dataclass
class ObjectRecord:
    bucket: str
    key: str
    sha256: str
    size: int
    preexisting_absent: bool = False
    attempted: bool = False
    created: bool = False
    deleted: bool = False


class P03Resources:
    def __init__(self, database):
        self.database = database
        self.test_id = uuid4().hex
        self.prefix = f"expert:test:p03:{self.test_id}:"
        self.stream = self.prefix + "ingestion.jobs.v1"
        self.group = f"p03-ingestion-{self.test_id}"
        self.owner_key = f"expert:test:p03:{self.test_id}:owner"
        self._owner_nonce = uuid4().hex
        self._marker_attempted = False
        self._marker_created = False
        self._stream_attempted = False
        self._stream_created = False
        self._owned_streams = {self.stream}
        self._notifications: dict[str, dict] = {}
        self._objects: dict[tuple[str, str], ObjectRecord] = {}
        self._iam_probe_keys: set[tuple[str, str]] = set()
        self._owned_debug_runs: set[UUID] = set()
        self._redis_clients = []
        self._s3_clients = []
        self.s3_backend: Any = None
        self.s3_ingest: Any = None
        self.redis_backend: Any = None
        self.redis_ingest: Any = None
        self.redis_outbox: Any = None
        self.redis_urls: dict[str, SecretStr] = {}
        self.events = []

    def __repr__(self):
        return f"P03Resources(test_id={self.test_id!r}, database={self.database.dbname!r})"

    def _event(self, event: str, **fields):
        self.events.append({"event": event, **fields})
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        (EVIDENCE / f"lifecycle-{self.test_id}.json").write_text(json.dumps({
            "scope": "real service-role S3/Redis fixture; synthetic data only",
            "test_id": self.test_id, "database": self.database.dbname,
            "stream": self.stream, "group": self.group, "events": self.events,
        }, indent=2) + "\n", encoding="utf-8")

    def _guard_s3(self, params, model, context, *, role: str | None = None, **kwargs):
        __tracebackhide__ = True
        operation = model.name
        bucket = params.get("Bucket")
        if bucket == "debug" and role != "backend":
            raise ResourceFixtureError("P11_S3_DEBUG_ROLE_BLOCKED")
        if operation == "HeadBucket" and bucket in BUCKETS:
            return
        key = params.get("Key")
        if (bucket, key) in self._iam_probe_keys:
            if role != "backend" or operation not in {"HeadObject", "DeleteObject"}:
                raise ResourceFixtureError("P14_IAM_PROBE_OPERATION_BLOCKED")
            # A minted, preflight-absent exact key reaches real IAM. No PUT or
            # broad-prefix exception is introduced and there is nothing to clean.
            context["p03_iam_probe_key"] = (bucket, key)
            return
        record = self._objects.get((bucket, key))
        if record is None or operation not in {"HeadObject", "GetObject", "PutObject", "DeleteObject"}:
            raise ResourceFixtureError("P03_S3_RESOURCE_ESCAPE_BLOCKED")
        if operation == "PutObject":
            if not record.preexisting_absent or params.get("IfNoneMatch") != "*":
                raise ResourceFixtureError("P03_S3_UNCONDITIONAL_WRITE_BLOCKED")
            record.attempted = True
        if operation == "DeleteObject" and not (record.preexisting_absent and record.attempted):
            raise ResourceFixtureError("P03_S3_UNOWNED_DELETE_BLOCKED")
        context["p03_resource_key"] = (bucket, key)

    def _s3_result(self, http_response, parsed, model, context, **kwargs):
        probe = context.get("p03_iam_probe_key")
        if probe in self._iam_probe_keys:
            self._event("iam_probe_result", bucket=probe[0], key=probe[1],
                        operation=model.name, status=http_response.status_code)
        record = self._objects.get(context.get("p03_resource_key"))
        if record and 200 <= http_response.status_code < 300:
            if model.name == "PutObject":
                record.created = True
                self._event("object_put", bucket=record.bucket, key=record.key, sha256=record.sha256, size=record.size)
            elif model.name == "DeleteObject":
                record.deleted = True
                self._event("object_deleted", bucket=record.bucket, key=record.key)

    async def open(self):
        __tracebackhide__ = True
        if (self.database.host, self.database.port) != ("127.0.0.1", 25432) or not re.fullmatch(
                r"expert_test_p02_[0-9a-f]{12}", self.database.dbname):
            raise ResourceFixtureError("P03_DATABASE_TARGET_INVALID")
        # Administrative S3 client is read-only and closed before any fixture write.
        root_user = _secret("minio_root_user")
        if root_user.get_secret_value() != "expert_storage_admin":
            raise ResourceFixtureError("P03_S3_ADMIN_IDENTITY_INVALID")
        admin = _s3_client(root_user, _secret("minio_root_password"))
        try:
            for bucket in sorted(BUCKETS):
                if admin.get_bucket_versioning(Bucket=bucket).get("Status"):
                    raise ResourceFixtureError("P03_S3_VERSIONING_UNSUPPORTED")
                try:
                    lock = admin.get_object_lock_configuration(Bucket=bucket)
                except ClientError as error:
                    if error.response.get("Error", {}).get("Code") != "ObjectLockConfigurationNotFoundError":
                        raise ResourceFixtureError("P03_S3_LOCK_PREFLIGHT_FAILED") from None
                else:
                    if lock.get("ObjectLockConfiguration", {}).get("ObjectLockEnabled"):
                        raise ResourceFixtureError("P03_S3_OBJECT_LOCK_UNSUPPORTED")
        finally:
            admin.close()
        self._event("s3_read_only_preflight", versioning_disabled=True, object_lock_disabled=True)
        for role in ("backend", "ingest"):
            access = _secret(f"{role}_s3_access_key")
            if access.get_secret_value() != "expert_" + role:
                raise ResourceFixtureError("P03_S3_ROLE_INVALID")
            client = _s3_client(access, _secret(f"{role}_s3_secret_key"))
            client.meta.events.register("before-parameter-build.s3", partial(self._guard_s3, role=role))
            client.meta.events.register("after-call.s3", self._s3_result)
            self._s3_clients.append(client)
            setattr(self, f"s3_{role}", client)
            for bucket in sorted(BUCKETS):
                # Debug grants only exact-key object access, not ListBucket
                # (which S3 HeadBucket requires). Admin already checked it above.
                if bucket == "debug":
                    continue
                client.head_bucket(Bucket=bucket)
        for role in ("backend", "ingest", "outbox"):
            url, options = _redis_credentials(role)
            self.redis_urls[role] = url
            client = ScopedRedis(stream=self.stream, owner_key=self.owner_key, group=self.group,
                                 owned_streams=self._owned_streams, notifications=self._notifications, **options)
            self._redis_clients.append(client)
            setattr(self, f"redis_{role}", client)
            await client.ping()
        self._marker_attempted = True
        if not await self.redis_backend.set(self.owner_key, self._owner_nonce, nx=True, ex=7200):
            raise ResourceFixtureError("P03_REDIS_OWNER_COLLISION")
        self._marker_created = True
        try:
            absent = await self.redis_backend.get(self.stream) is None
        except ResponseError:
            absent = False
        if not absent:
            raise ResourceFixtureError("P03_REDIS_STREAM_COLLISION")
        self._stream_attempted = True
        await self.redis_ingest.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
        self._stream_created = True
        await self.redis_backend.expire(self.stream, 3600)
        self._event("redis_created", owner_claimed=True, ttl_seconds=3600)

    async def _require_redis_owner(self):
        if not self._marker_created or await self.redis_backend.get(self.owner_key) != self._owner_nonce:
            raise ResourceFixtureError("P03_REDIS_OWNERSHIP_CHANGED")

    async def register_notification_stream(self, key: str) -> str:
        """Reserve an exact stream key before the real outbox publisher uses it."""
        await self._require_redis_owner()
        if not key.startswith(self.prefix):
            raise ResourceFixtureError("P03_REDIS_KEY_ESCAPE_BLOCKED")
        suffix = key[len(self.prefix):]
        if suffix != "knowledge.events":
            match = re.fullmatch(r"(?:ingestion|run)\.events:([0-9a-f-]{36})", suffix)
            if match is None or str(UUID(match[1])) != match[1]:
                raise ResourceFixtureError("P03_REDIS_NOTIFICATION_IDENTITY_INVALID")
        if key in self._notifications:
            return key
        record = {"preexisting_absent": False, "attempted": False, "created": False}
        self._notifications[key] = record
        self._owned_streams.add(key)
        try:
            if await self.redis_backend.get(key) is not None:
                raise ResourceFixtureError("P03_REDIS_NOTIFICATION_PREEXISTS")
            record["preexisting_absent"] = True
        except Exception:
            self._owned_streams.remove(key)
            self._notifications.pop(key)
            raise ResourceFixtureError("P03_REDIS_NOTIFICATION_RESERVATION_FAILED") from None
        self._event("notification_reserved", stream=key)
        return key

    async def drop_ingestion_stream(self):
        """Simulate loss of only the owned command stream for reconciliation tests."""
        await self._require_redis_owner()
        groups = await self.redis_ingest.xinfo_groups(self.stream)
        if not self._stream_created or len(groups) != 1 or groups[0]["name"] != self.group:
            raise ResourceFixtureError("P03_REDIS_GROUP_IDENTITY_CHANGED")
        await self.redis_backend.delete(self.stream)
        self._stream_created = False
        if await self.redis_backend.get(self.stream) is not None:
            raise ResourceFixtureError("P03_REDIS_STREAM_DROP_FAILED")
        self._event("redis_stream_dropped_for_reconciliation", stream_absent=True)

    async def ensure_ingestion_group(self):
        """Restore the exact owned group after the test's deliberate stream loss."""
        await self._require_redis_owner()
        self._stream_attempted = True
        try:
            await self.redis_ingest.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
        except ResponseError:
            groups = await self.redis_ingest.xinfo_groups(self.stream)
            if len(groups) != 1 or groups[0]["name"] != self.group:
                raise ResourceFixtureError("P03_REDIS_GROUP_IDENTITY_CHANGED") from None
        self._stream_created = True
        await self.redis_backend.expire(self.stream, 3600)
        self._event("redis_group_restored", ttl_seconds=3600)

    def register_original(self, document_id: UUID, version_id: UUID, sha256: str, size: int) -> str:
        """Call after IDs are reserved, before real PUT. Existing keys are refused."""
        __tracebackhide__ = True
        key = f"originals/{document_id}/{version_id}/{sha256}.pdf"
        match = ORIGINAL_KEY.fullmatch(key)
        if not match or size <= 0 or size > 50 * 1024 * 1024:
            raise ResourceFixtureError("P03_S3_ORIGINAL_IDENTITY_INVALID")
        if str(UUID(match[1])) != match[1] or str(UUID(match[2])) != match[2]:
            raise ResourceFixtureError("P03_S3_ORIGINAL_IDENTITY_INVALID")
        identity = ("originals", key)
        if identity in self._objects:
            old = self._objects[identity]
            if (old.sha256, old.size) != (sha256, size):
                raise ResourceFixtureError("P03_S3_LEDGER_CONFLICT")
            return key
        record = ObjectRecord("originals", key, sha256, size)
        self._objects[identity] = record
        try:
            head = self.head("originals", key)
            if head is not None:
                raise ResourceFixtureError("P03_S3_KEY_PREEXISTS")
            record.preexisting_absent = True
            self._event("object_reserved", bucket="originals", key=key, sha256=sha256, size=size)
            return key
        except Exception:
            self._objects.pop(identity, None)
            raise

    def mint_owned_iam_probe_key(self) -> str:
        """Mint one absent exact key for a real backend IAM denial, without PUT."""
        __tracebackhide__ = True
        key = f"iam-probe/{self.test_id}/{uuid4().hex}"
        root_user = _secret("minio_root_user")
        if root_user.get_secret_value() != "expert_storage_admin":
            raise ResourceFixtureError("P03_S3_ADMIN_IDENTITY_INVALID")
        admin = _s3_client(root_user, _secret("minio_root_password"))
        try:
            try:
                admin.head_object(Bucket="artifacts", Key=key)
            except ClientError as error:
                if (error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404
                        or error.response.get("Error", {}).get("Code") not in {"404", "NotFound", "NoSuchKey"}):
                    raise ResourceFixtureError("P14_IAM_PROBE_PREFLIGHT_FAILED") from None
            else:
                raise ResourceFixtureError("P03_S3_KEY_PREEXISTS")
        finally:
            admin.close()
        self._iam_probe_keys.add(("artifacts", key))
        self._event("iam_probe_reserved_absent", bucket="artifacts", key=key)
        return key

    def register_parse_artifact(self, intent) -> str:
        """Reserve the exact parse artifact key from an SQL intent before real PUT."""
        __tracebackhide__ = True
        try:
            intent_id = UUID(str(intent["id"]))
            version_id = UUID(str(intent["document_version_id"]))
            generation_id = UUID(str(intent["parse_generation_id"]))
            sha256 = intent["sha256"]
            size = intent["size_bytes"]
            role = intent["artifact_role"]
            slot = intent["slot"]
            bucket = intent["bucket"]
            object_key = intent["object_key"]
            media_type = intent["media_type"]
        except Exception:
            raise ResourceFixtureError("P03_S3_PARSE_ARTIFACT_IDENTITY_INVALID") from None
        if not isinstance(sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
            raise ResourceFixtureError("P03_S3_PARSE_ARTIFACT_IDENTITY_INVALID")
        if role == "canonical" and slot == "document" and media_type == "application/json":
            suffix = "json"
        elif role == "source_crop" and media_type == "image/png":
            suffix = "png"
        else:
            raise ResourceFixtureError("P03_S3_PARSE_ARTIFACT_IDENTITY_INVALID")
        key = f"parses/{version_id}/{generation_id}/{intent_id}/{sha256}.{suffix}"
        match = PARSE_ARTIFACT_KEY.fullmatch(key)
        if (
            bucket != "artifacts"
            or object_key != key
            or not match
            or isinstance(size, bool)
            or not isinstance(size, int)
            or not 0 < size <= 256 * 1024 * 1024
        ):
            raise ResourceFixtureError("P03_S3_PARSE_ARTIFACT_IDENTITY_INVALID")
        if str(UUID(match[1])) != match[1] or str(UUID(match[2])) != match[2] or str(UUID(match[3])) != match[3]:
            raise ResourceFixtureError("P03_S3_PARSE_ARTIFACT_IDENTITY_INVALID")
        identity = ("artifacts", key)
        if identity in self._objects:
            old = self._objects[identity]
            if (old.sha256, old.size) != (sha256, size):
                raise ResourceFixtureError("P03_S3_LEDGER_CONFLICT")
            return key
        record = ObjectRecord("artifacts", key, sha256, size)
        self._objects[identity] = record
        try:
            head = self.head("artifacts", key)
            if head is not None:
                raise ResourceFixtureError("P03_S3_KEY_PREEXISTS")
            record.preexisting_absent = True
            self._event("parse_artifact_reserved", bucket="artifacts", key=key, sha256=sha256, size=size)
            return key
        except Exception:
            self._objects.pop(identity, None)
            raise

    def reserve_debug_run_id(self) -> UUID:
        """Mint a new run identity owned only by this disposable resource fixture."""
        run_id = uuid4()
        self._owned_debug_runs.add(run_id)
        return run_id

    def register_debug_object(self, intent) -> str:
        """Register one committed SQL capture reservation before conditional PUT."""
        __tracebackhide__ = True
        try:
            reference = DebugObjectRef.model_validate(intent)
            run_id = UUID(reference.object_key.split("/")[1])
            if (run_id not in self._owned_debug_runs or reference.state not in {"reserved", "attached"}
                    or reference.object_version_id is not None):
                raise ValueError()
        except Exception:
            raise ResourceFixtureError("P11_S3_DEBUG_IDENTITY_INVALID") from None
        identity = (reference.bucket, reference.object_key)
        previous = self._objects.get(identity)
        if previous is not None:
            if (previous.sha256, previous.size) != (reference.sha256, reference.size_bytes):
                raise ResourceFixtureError("P03_S3_LEDGER_CONFLICT")
            return reference.object_key
        record = ObjectRecord(reference.bucket, reference.object_key, reference.sha256, reference.size_bytes)
        self._objects[identity] = record
        try:
            if self.head(reference.bucket, reference.object_key) is not None:
                raise ResourceFixtureError("P03_S3_KEY_PREEXISTS")
            record.preexisting_absent = True
            self._event("debug_object_reserved", bucket=record.bucket, key=record.key,
                        sha256=record.sha256, size=record.size)
            return reference.object_key
        except Exception:
            self._objects.pop(identity, None)
            raise

    def head(self, bucket: str, key: str):
        __tracebackhide__ = True
        client = self.s3_ingest if bucket == "artifacts" else self.s3_backend
        try:
            return client.head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return None
            raise ResourceFixtureError("P03_S3_HEAD_FAILED") from None

    def delete_owned(self, bucket: str, key: str):
        __tracebackhide__ = True
        record = self._objects.get((bucket, key))
        if record is None or not record.preexisting_absent or not record.attempted:
            raise ResourceFixtureError("P03_S3_UNOWNED_DELETE_BLOCKED")
        client = self.s3_ingest if bucket == "artifacts" else self.s3_backend
        head = self.head(bucket, key)
        if head is None:
            record.deleted = True
            return
        if head.get("VersionId") or head.get("ContentLength") != record.size:
            raise ResourceFixtureError("P03_S3_OBJECT_IDENTITY_CHANGED")
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        try:
            value = body.read(record.size + 1)
        finally:
            body.close()
        if len(value) != record.size or hashlib.sha256(value).hexdigest() != record.sha256:
            raise ResourceFixtureError("P03_S3_OBJECT_HASH_CHANGED")
        client.delete_object(Bucket=bucket, Key=key, IfMatch=head["ETag"])
        if self.head(bucket, key) is not None:
            raise ResourceFixtureError("P03_S3_CLEANUP_INCOMPLETE")
        self._event("object_absence_verified", bucket=bucket, key=key)

    async def _close_redis(self):
        if not self._marker_attempted:
            return
        marker = await self.redis_backend.get(self.owner_key)
        if marker is None and not self._marker_created:
            self._event("redis_claim_absent", owner_marker_absent=True)
            return
        if marker != self._owner_nonce:
            raise ResourceFixtureError("P03_REDIS_OWNERSHIP_CHANGED")
        for key, record in self._notifications.items():
            if not record["attempted"]:
                continue
            if not record["preexisting_absent"]:
                raise ResourceFixtureError("P03_REDIS_UNOWNED_DELETE_BLOCKED")
            try:
                groups = await self.redis_ingest.xinfo_groups(key)
            except ResponseError:
                if await self.redis_backend.get(key) is not None:
                    raise ResourceFixtureError("P03_REDIS_NOTIFICATION_IDENTITY_CHANGED") from None
            else:
                if groups:
                    raise ResourceFixtureError("P03_REDIS_NOTIFICATION_GROUP_CHANGED")
                await self.redis_backend.delete(key)
            if await self.redis_backend.get(key) is not None:
                raise ResourceFixtureError("P03_REDIS_NOTIFICATION_CLEANUP_FAILED")
            self._event("notification_deleted", stream=key, stream_absent=True)
        if self._stream_attempted and not self._stream_created:
            try:
                groups = await self.redis_ingest.xinfo_groups(self.stream)
            except ResponseError:
                if await self.redis_backend.get(self.stream) is not None:
                    raise ResourceFixtureError("P03_REDIS_STREAM_IDENTITY_CHANGED") from None
                groups = []
            if groups:
                if len(groups) != 1 or groups[0]["name"] != self.group:
                    raise ResourceFixtureError("P03_REDIS_GROUP_IDENTITY_CHANGED")
                self._stream_created = True
        if self._stream_created:
            groups = await self.redis_ingest.xinfo_groups(self.stream)
            if len(groups) != 1 or groups[0]["name"] != self.group:
                raise ResourceFixtureError("P03_REDIS_GROUP_IDENTITY_CHANGED")
            await self.redis_backend.delete(self.stream)
        if self._stream_attempted and await self.redis_backend.get(self.stream) is not None:
            raise ResourceFixtureError("P03_REDIS_CLEANUP_INCOMPLETE")
        await self.redis_backend.delete(self.owner_key)
        if await self.redis_backend.get(self.owner_key) is not None:
            raise ResourceFixtureError("P03_REDIS_MARKER_CLEANUP_INCOMPLETE")
        self._event("redis_deleted", stream_absent=True if self._stream_attempted else None,
                    owner_marker_absent=True)

    async def close(self):
        __tracebackhide__ = True
        failures = []
        try:
            for record in self._objects.values():
                if record.attempted:
                    try:
                        self.delete_owned(record.bucket, record.key)
                    except Exception:
                        failures.append("object_cleanup")
            try:
                await self._close_redis()
            except Exception:
                failures.append("redis_cleanup")
        finally:
            for client in self._redis_clients:
                await client.aclose()
            for client in self._s3_clients:
                client.close()
        self._event("fixture_closed", cleanup_failures=failures)
        if failures:
            raise ResourceFixtureError("P03_RESOURCE_CLEANUP_FAILED")


@pytest_asyncio.fixture
async def p03_resources(request):
    __tracebackhide__ = True
    if os.environ.get("EXPERT_INTEGRATION_TESTS") != "1" or os.environ.get("EXPERT_P03_RESOURCE_TESTS") != "1":
        pytest.fail("P03 resources require EXPERT_INTEGRATION_TESTS=1 and EXPERT_P03_RESOURCE_TESTS=1", pytrace=False)
    database = request.getfixturevalue("isolated_database")
    resource = P03Resources(database)
    try:
        await resource.open()
    except Exception as error:
        resource._event("setup_failed", error_type=type(error).__name__,
                        reason=str(error) if isinstance(error, ResourceFixtureError) else "PROVIDER_SETUP_FAILED")
        await resource.close()
        raise ResourceFixtureError("P03_RESOURCE_SETUP_FAILED") from None
    try:
        yield resource
    finally:
        await resource.close()
