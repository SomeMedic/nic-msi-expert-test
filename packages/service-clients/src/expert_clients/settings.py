"""Fail-fast runtime configuration; secret values never enter validation errors."""
from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Literal, Self

from pydantic import AnyHttpUrl, Field, SecretStr, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ServiceName = Literal["backend", "agent-runtime", "ingestion-worker", "retrieval-ml", "outbox-publisher"]
SECRET_FIELDS = (
    "database_dsn", "redis_url", "s3_access_key", "s3_secret_key",
    "agent_runtime_token", "retrieval_ml_token", "llm_api_key",
    "viewer_access_key", "operator_access_key", "admin_access_key",
)


class ConfigurationError(RuntimeError):
    """Contains field names only, never input values or secret paths."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EXPERT_", extra="forbid", hide_input_in_errors=True,
        env_file=None, frozen=True, allow_inf_nan=False,
    )
    service_name: ServiceName
    app_env: Literal["local", "test"] = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    port: int = Field(default=8000, ge=1, le=65535)
    public_base_url: AnyHttpUrl = AnyHttpUrl("http://localhost:8080")
    internal_api_version: Literal["v1"] = "v1"
    database_dsn: SecretStr | None = None
    database_dsn_file: Path | None = None
    redis_url: SecretStr | None = None
    redis_url_file: Path | None = None
    s3_endpoint: AnyHttpUrl | None = None
    s3_access_key: SecretStr | None = None
    s3_access_key_file: Path | None = None
    s3_secret_key: SecretStr | None = None
    s3_secret_key_file: Path | None = None
    s3_bucket_originals: str = "originals"
    s3_bucket_artifacts: str = "artifacts"
    s3_bucket_debug: str = "debug"
    agent_runtime_url: AnyHttpUrl = AnyHttpUrl("http://agent-runtime:8000")
    backend_internal_url: AnyHttpUrl = AnyHttpUrl("http://backend:8000")
    runtime_manifest_path: Path = Path("config/runtime/manifest.json")
    outbox_publisher_url: AnyHttpUrl = AnyHttpUrl("http://outbox-publisher:8000")
    retrieval_ml_url: AnyHttpUrl = AnyHttpUrl("http://retrieval-ml:8000")
    llm_base_url: AnyHttpUrl = AnyHttpUrl("http://vllm:8000/v1")
    agent_runtime_token: SecretStr | None = None
    agent_runtime_token_file: Path | None = None
    retrieval_ml_token: SecretStr | None = None
    retrieval_ml_token_file: Path | None = None
    llm_api_key: SecretStr | None = None
    llm_api_key_file: Path | None = None
    viewer_access_key: SecretStr | None = None
    viewer_access_key_file: Path | None = None
    operator_access_key: SecretStr | None = None
    operator_access_key_file: Path | None = None
    admin_access_key: SecretStr | None = None
    admin_access_key_file: Path | None = None
    auth_session_seconds: int = Field(default=28_800, ge=60, le=86_400)
    http_connect_timeout_seconds: float = Field(default=3, gt=0, le=30)
    http_pool_timeout_seconds: float = Field(default=5, gt=0, le=60)
    run_deadline_seconds: int = Field(default=300, gt=0)
    run_lease_seconds: int = Field(default=60, gt=0)
    run_heartbeat_seconds: int = Field(default=10, gt=0)
    run_max_recovery_attempts: int = Field(default=2, ge=0, le=10)
    run_global_concurrency: int = Field(default=1, ge=1, le=8)
    run_max_active_per_operator: int = Field(default=1, ge=1, le=8)
    sse_heartbeat_seconds: float = Field(default=15, gt=0)
    sse_pg_poll_seconds: float = Field(default=1, gt=0)
    sse_batch_size: int = Field(default=100, ge=1, le=1000)
    sse_buffer_max_events: int = Field(default=256, ge=1, le=4096)
    upload_max_bytes: int = Field(default=52_428_800, ge=1, le=52_428_800)
    upload_body_max_bytes: int = Field(default=54_525_952, ge=1)
    upload_max_concurrency: int = Field(default=2, ge=1, le=4)
    upload_timeout_seconds: float = Field(default=120, gt=0, le=600)
    upload_metadata_max_bytes: int = Field(default=65_536, ge=1024, le=1_048_576)
    upload_cleanup_poll_seconds: float = Field(default=30, ge=1, le=3600)
    upload_cleanup_lease_seconds: int = Field(default=60, ge=30, le=300)
    upload_cleanup_operation_timeout_seconds: float = Field(default=10, gt=0, le=30)
    pdf_max_pages: int = Field(default=500, ge=1)
    ingestion_max_queued: int = Field(default=10, ge=1)
    ingestion_concurrency: int = Field(default=1, ge=1, le=4)
    ingestion_max_attempts: int = Field(default=3, ge=1, le=10)
    ingestion_job_timeout_seconds: int = Field(default=600, gt=0, le=3600)
    ingestion_pipeline_fingerprint: str = Field(default="canonical-v1", pattern=r"^[A-Za-z0-9:._-]{1,200}$")
    ingestion_lease_seconds: int = Field(default=60, ge=1, le=300)
    ingestion_heartbeat_seconds: float = Field(default=10, gt=0)
    ingestion_temp_root: Path = Field(default_factory=lambda: Path(tempfile.gettempdir()) / "expert-ingestion")
    ingestion_download_timeout_seconds: float = Field(default=60, gt=0, le=300)
    ingestion_parser_python_path: Path = Path("/app/.venv-parser/bin/python")
    ingestion_parser_profile_path: Path = Path("infra/compose/parser-profile.json")
    ingestion_parser_assets_lock_path: Path = Path("parser-assets.lock.json")
    ingestion_region_reviews_path: Path = Path("config/parsing/region-reviews.json")
    ingestion_reclaim_idle_seconds: int = Field(default=60, ge=1, le=3600)
    ingestion_reconcile_seconds: float = Field(default=60, ge=1, le=3600)
    outbox_batch_size: int = Field(default=50, ge=1, le=500)
    outbox_claim_seconds: int = Field(default=30, gt=0)
    outbox_poll_seconds: float = Field(default=1, gt=0)
    outbox_max_attempts: Literal[8] = 8
    outbox_publish_concurrency: int = Field(default=4, ge=1, le=16)
    outbox_operation_timeout_seconds: float = Field(default=5, gt=0, le=60)
    outbox_shutdown_seconds: float = Field(default=10, gt=0, le=60)
    outbox_retry_base_seconds: float = Field(default=1, gt=0, le=60)
    outbox_retry_max_seconds: int = Field(default=60, ge=1, le=3600)
    outbox_reconcile_seconds: float = Field(default=60, ge=1, le=3600)
    outbox_reconcile_batch_size: int = Field(default=100, ge=1, le=1000)
    redis_stream_ingestion: str = Field(default="expert:ingestion.jobs.v1", pattern=r"^expert:[A-Za-z0-9:._-]{1,240}$")
    redis_notification_prefix: str = Field(default="expert:", pattern=r"^expert:(?:[A-Za-z0-9:._-]{0,199}:)?$")
    model_offline_mode: Literal[True] = True
    llm_model: Literal["Qwen/Qwen3-14B-AWQ"] = "Qwen/Qwen3-14B-AWQ"
    llm_revision: str | None = None
    llm_enable_thinking: Literal[False] = False
    llm_max_model_len: Literal[8192] = 8192
    llm_max_num_seqs: Literal[1] = 1
    embedding_model: Literal["ai-forever/FRIDA"] = "ai-forever/FRIDA"
    embedding_revision: str | None = None
    embedding_model_path: Path | None = None
    embedding_dimension: Literal[1536] = 1536
    embedding_max_input_tokens: Literal[512] = 512
    embedding_max_batch_items: int = Field(default=16, ge=1, le=16, strict=True)
    embedding_max_batch_tokens: int = Field(default=8192, ge=512, le=8192, strict=True)
    embedding_normalize: Literal[True] = True
    embedding_device: Literal["cpu"] = "cpu"
    reranker_model: Literal["Qwen/Qwen3-Reranker-0.6B"] = "Qwen/Qwen3-Reranker-0.6B"
    reranker_revision: str | None = None
    reranker_model_path: Path | None = None
    reranker_max_input_tokens: int = Field(default=2048, ge=1, le=2048, strict=True)
    reranker_max_batch_items: int = Field(default=16, ge=1, le=16, strict=True)
    reranker_max_batch_tokens: int = Field(default=8192, ge=2048, le=8192, strict=True)
    reranker_device: Literal["cpu", "cuda"] = "cpu"
    reranker_cuda_python: Path = Path("/app/.venv-reranker-cuda/bin/python")
    reranker_cuda_max_batch_tokens: int = Field(default=2048, ge=2048, le=8192, strict=True)
    reranker_score_type: Literal["sigmoid"] = "sigmoid"
    models_lock_path: Path = Path("models.lock.json")
    ml_cpu_threads: int = Field(default=8, ge=1, le=16, strict=True)
    ml_admission_slots: int = Field(default=2, ge=1, le=2, strict=True)
    ml_request_timeout_seconds: float = Field(default=120, ge=1, le=300)
    docling_artifacts_path: Path | None = None
    otel_exporter_otlp_endpoint: AnyHttpUrl | None = None
    otel_service_name: str | None = None
    jaeger_public_base_url: AnyHttpUrl = AnyHttpUrl("http://localhost:16687")
    debug_capture_default: Literal[False] = False
    debug_capture_allowed: bool = False
    debug_capture_ttl_hours: int = Field(default=24, ge=1, le=168)
    private_step_artifact_retention_hours: int = Field(default=168, ge=1, le=8760)
    source_purge_retention_seconds: int = Field(default=3600, ge=3600, le=31536000)
    source_purge_plan_ttl_seconds: int = Field(default=900, ge=60, le=3600)
    purge_cleanup_poll_seconds: float = Field(default=30, ge=1, le=3600)
    purge_cleanup_lease_seconds: int = Field(default=30, ge=30, le=30)
    purge_cleanup_operation_timeout_seconds: float = Field(default=3, gt=0, le=5)
    purge_cleanup_batch_size: int = Field(default=20, ge=1, le=20)

    @field_validator("ml_cpu_threads", "ml_admission_slots", "embedding_max_batch_items",
                     "embedding_max_batch_tokens", "reranker_max_input_tokens",
                     "reranker_max_batch_items", "reranker_max_batch_tokens",
                     "reranker_cuda_max_batch_tokens", mode="before")
    @classmethod
    def integer_configuration(cls, value):
        # Environment variables are text. Accept only canonical unsigned integer
        # strings; strict field validation still rejects bool, floats and coercion.
        if isinstance(value, str) and re.fullmatch(r"(?:0|[1-9][0-9]{0,5})", value):
            return int(value)
        return value

    @model_validator(mode="after")
    def validate_sources(self) -> Self:
        for name in ("public_base_url", "s3_endpoint", "agent_runtime_url", "backend_internal_url", "outbox_publisher_url", "retrieval_ml_url", "llm_base_url", "otel_exporter_otlp_endpoint", "jaeger_public_base_url"):
            value = getattr(self, name)
            if value is not None and any((value.username, value.password, value.query, value.fragment)):
                raise ConfigurationError(f"URL credentials, query and fragment forbidden: {name}")
        for name in SECRET_FIELDS:
            value = getattr(self, name)
            source = getattr(self, f"{name}_file")
            if value is not None and source is not None:
                raise ConfigurationError(f"Conflicting secret sources: {name}")
            if source is not None:
                try:
                    if not source.is_file() or source.stat().st_size > 16_384:
                        raise OSError()
                    value = SecretStr(source.read_text(encoding="utf-8").strip())
                except (OSError, UnicodeError):
                    raise ConfigurationError(f"Unreadable secret source: {name}") from None
                object.__setattr__(self, name, value)
            if value is not None and not value.get_secret_value().strip():
                raise ConfigurationError(f"Empty secret: {name}")
        if self.run_heartbeat_seconds >= self.run_lease_seconds:
            raise ConfigurationError("run_heartbeat_seconds must be less than run_lease_seconds")
        if self.ingestion_heartbeat_seconds >= self.ingestion_lease_seconds:
            raise ConfigurationError("ingestion_heartbeat_seconds must be less than ingestion_lease_seconds")
        if self.outbox_operation_timeout_seconds >= self.outbox_claim_seconds:
            raise ConfigurationError("outbox_operation_timeout_seconds must be less than outbox_claim_seconds")
        if self.outbox_retry_base_seconds > self.outbox_retry_max_seconds:
            raise ConfigurationError("outbox_retry_base_seconds must not exceed outbox_retry_max_seconds")
        if self.upload_body_max_bytes <= self.upload_max_bytes:
            raise ConfigurationError("upload_body_max_bytes must include multipart overhead")
        if self.upload_cleanup_operation_timeout_seconds >= self.upload_cleanup_lease_seconds / 3:
            raise ConfigurationError("upload cleanup operation timeout must be less than one third of its lease")
        for name in ("s3_bucket_originals", "s3_bucket_artifacts", "s3_bucket_debug"):
            if not getattr(self, name).strip():
                raise ConfigurationError(f"Empty bucket: {name}")
        return self

    def validate_service(self) -> None:
        required: list[str] = []
        if self.service_name != "retrieval-ml":
            required += ["database_dsn", "redis_url"]
        if self.service_name in {"backend", "ingestion-worker"}:
            required += ["s3_endpoint", "s3_access_key", "s3_secret_key"]
        if self.service_name in {"backend", "agent-runtime"}:
            required += ["agent_runtime_token"]
        if self.service_name == "backend":
            required += ["viewer_access_key", "operator_access_key", "admin_access_key"]
        if self.service_name in {"agent-runtime", "ingestion-worker", "retrieval-ml"}:
            required += ["retrieval_ml_token"]
        if self.service_name == "agent-runtime":
            required += ["llm_api_key", "llm_revision"]
        if self.service_name == "retrieval-ml":
            required += ["embedding_model_path", "embedding_revision", "reranker_model_path", "reranker_revision"]
            if self.database_dsn is not None:
                raise ConfigurationError("retrieval-ml must not receive database_dsn")
        missing = [name for name in required if getattr(self, name) is None]
        if missing:
            raise ConfigurationError("Missing configuration fields: " + ", ".join(missing))
        if self.service_name == "backend":
            access_keys = [self.require_secret(f"{role}_access_key").get_secret_value() for role in ("viewer", "operator", "admin")]
            if any(len(key) < 32 for key in access_keys) or len(set(access_keys)) != 3:
                raise ConfigurationError("Application access keys must be distinct and at least 32 characters")
        for name in ("llm_revision", "embedding_revision", "reranker_revision"):
            value = getattr(self, name)
            if value is not None and (len(value) != 40 or any(c not in "0123456789abcdef" for c in value)):
                raise ConfigurationError(f"Expected immutable revision: {name}")

    def require_secret(self, name: str) -> SecretStr:
        if name not in SECRET_FIELDS:
            raise ConfigurationError("Unknown secret field")
        secret = getattr(self, name)
        if secret is None:
            raise ConfigurationError(f"Missing secret: {name}")
        return secret


def load_settings(service_name: ServiceName) -> Settings:
    if os.environ.get("EXPERT_SERVICE_NAME", service_name) != service_name:
        raise ConfigurationError("EXPERT_SERVICE_NAME conflicts with service entrypoint")
    try:
        settings = Settings(service_name=service_name)
        settings.validate_service()
        return settings
    except ValidationError as error:
        fields = sorted({str(item["loc"][0]) for item in error.errors() if item["loc"]})
        raise ConfigurationError("Invalid configuration fields: " + ", ".join(fields)) from None
