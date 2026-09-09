"""Dedicated local application keys and opaque, expiring PostgreSQL sessions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import re
import secrets
from urllib.parse import urlsplit

from fastapi import Request
from psycopg.rows import dict_row

from expert_clients.settings import Settings
from expert_contracts.auth import ApplicationRole, SessionInfo
from expert_contracts.errors import ErrorCode
from expert_api.errors import ApiError, database_failures

COOKIE_NAME = "expert_session"
_SESSION_TOKEN = re.compile(r"[A-Za-z0-9_-]{43}\Z")
_ROLE_LEVEL = {ApplicationRole.VIEWER: 0, ApplicationRole.OPERATOR: 1, ApplicationRole.ADMIN: 2}


@dataclass(frozen=True)
class Principal:
    subject: str
    role: ApplicationRole
    expires_at: datetime | None = None

    def require(self, role: ApplicationRole) -> None:
        if _ROLE_LEVEL[self.role] < _ROLE_LEVEL[role]:
            raise ApiError(ErrorCode.FORBIDDEN)

    def public(self) -> SessionInfo:
        return SessionInfo(principal_id=self.subject, role=self.role, expires_at=self.expires_at)


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def _origin(url: str) -> tuple[str, str | None, int | None]:
    parsed = urlsplit(url)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Invalid origin")
    return parsed.scheme, parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)


class AuthService:
    def __init__(self, settings: Settings, pool):
        self.settings = settings
        self.pool = pool

    def validate_origin(self, request: Request, *, required: bool) -> None:
        origin = request.headers.get("origin")
        if origin is None and not required:
            return
        try:
            allowed = _origin(str(self.settings.public_base_url))
            provided = _origin(origin or "")
            host_matches = provided[1] == allowed[1] or {provided[1], allowed[1]} == {"localhost", "127.0.0.1"}
            if not host_matches or provided[0] != allowed[0] or provided[2] != allowed[2]:
                raise ValueError("Origin mismatch")
            if urlsplit(origin or "").path not in ("", "/"):
                raise ValueError("Origin has a path")
        except ValueError:
            raise ApiError(ErrorCode.FORBIDDEN) from None

    def key_principal(self, key: str) -> Principal:
        if not 32 <= len(key) <= 256 or not key.isascii():
            raise ApiError(ErrorCode.UNAUTHENTICATED)
        role = None
        for candidate in ApplicationRole:
            expected = self.settings.require_secret(f"{candidate.value}_access_key").get_secret_value()
            if hmac.compare_digest(key, expected):
                role = candidate
        if role is None:
            raise ApiError(ErrorCode.UNAUTHENTICATED)
        return Principal(f"local-{role.value}", role)

    async def create_session(self, key: str) -> tuple[str, Principal]:
        principal = self.key_principal(key)
        token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=self.settings.auth_session_seconds)
        with database_failures():
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    await connection.execute("SELECT app.create_session(%s,%s,%s,%s)",
                                             (_token_hash(token), principal.subject, principal.role.value, expires_at))
        return token, Principal(principal.subject, principal.role, expires_at)

    async def authenticate(self, request: Request) -> Principal:
        authorization = request.headers.get("authorization")
        if authorization is not None:
            if not authorization.startswith("Bearer "):
                raise ApiError(ErrorCode.UNAUTHENTICATED)
            self.validate_origin(request, required=False)
            return self.key_principal(authorization[7:])
        token = request.cookies.get(COOKIE_NAME, "")
        if _SESSION_TOKEN.fullmatch(token) is None:
            raise ApiError(ErrorCode.UNAUTHENTICATED)
        self.validate_origin(request, required=request.method not in {"GET", "HEAD", "OPTIONS"})
        with database_failures():
            async with self.pool.connection() as connection:
                async with connection.cursor(row_factory=dict_row) as cursor:
                    row = await (await cursor.execute("SELECT * FROM app.lookup_session(%s)", (_token_hash(token),))).fetchone()
        if row is None or row["expires_at"] <= datetime.now(timezone.utc):
            raise ApiError(ErrorCode.UNAUTHENTICATED)
        return Principal(row["principal_id"], ApplicationRole(row["role"]), row["expires_at"])

    async def revoke_session(self, request: Request) -> None:
        self.validate_origin(request, required=True)
        token = request.cookies.get(COOKIE_NAME, "")
        if _SESSION_TOKEN.fullmatch(token) is None:
            return
        with database_failures():
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    await connection.execute("SELECT app.revoke_session(%s)", (_token_hash(token),))


def auth_service(request: Request) -> AuthService:
    service = getattr(request.app.state, "auth", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


async def current_principal(request: Request) -> Principal:
    return await auth_service(request).authenticate(request)
