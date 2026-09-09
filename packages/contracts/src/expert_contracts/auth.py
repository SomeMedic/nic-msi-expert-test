"""Local application authentication; credentials are input-only transport."""
from enum import StrEnum

from pydantic import Field, SecretStr

from .common import NonBlank, StrictDTO, UTCDateTime


class ApplicationRole(StrEnum):
    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"


class SessionRequest(StrictDTO):
    access_key: SecretStr = Field(min_length=32, max_length=256)


class SessionInfo(StrictDTO):
    principal_id: NonBlank = Field(max_length=200)
    role: ApplicationRole
    expires_at: UTCDateTime | None = None
