"""Sanitized availability DTOs; no deployment URLs, hardware paths or secrets."""
from typing import Literal

from pydantic import Field, model_validator

from .common import NonBlank, StrictDTO, UTCDateTime, unique
from .errors import ErrorCode


class HealthStatus(StrictDTO):
    status: Literal["ok", "not_ready"]
    service: NonBlank = Field(max_length=100)


class IngestionCapabilities(StrictDTO):
    pipeline_config_alias: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9:._-]{1,200}$")


class ComponentStatus(StrictDTO):
    name: NonBlank = Field(max_length=100)
    status: Literal["ready", "degraded", "unavailable"]
    error_code: ErrorCode | None = None


class SystemStatus(StrictDTO):
    status: Literal["ready", "degraded", "unavailable"]
    checked_at: UTCDateTime
    components: list[ComponentStatus] = Field(max_length=30)

    @model_validator(mode="after")
    def unique_components(self):
        unique([c.name for c in self.components], "component names")
        if self.status == "ready" and any(c.status != "ready" for c in self.components):
            raise ValueError("ready system contains non-ready components")
        return self
