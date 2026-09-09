"""Bounded peer readiness probe; provider bodies never become public diagnostics."""
import asyncio

import httpx

from expert_clients.dependencies import DependencyHealth
from expert_contracts.system import HealthStatus


class PeerHealth:
    def __init__(self, service: str, url: str, *, component: str, transport=None):
        self.service, self.component = service, component
        self.client = httpx.AsyncClient(base_url=url, trust_env=False, follow_redirects=False,
            timeout=httpx.Timeout(3), limits=httpx.Limits(max_connections=2, max_keepalive_connections=2), transport=transport)

    async def aclose(self) -> None:
        await self.client.aclose()

    async def check(self) -> DependencyHealth:
        try:
            async with asyncio.timeout(4):
                async with self.client.stream("GET", "/health/ready") as response:
                    if response.status_code != 200:
                        return DependencyHealth(self.component, False)
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(body) + len(chunk) > 16_384:
                            return DependencyHealth(self.component, False)
                        body.extend(chunk)
            result = HealthStatus.model_validate_json(body)
            return DependencyHealth(self.component, result.service == self.service and result.status == "ok")
        except Exception:
            return DependencyHealth(self.component, False)
