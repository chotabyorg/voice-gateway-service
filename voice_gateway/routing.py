"""Trusted PBX call registration and bounded Control Plane DID resolution."""

import asyncio
import json
import re
import time
from dataclasses import dataclass
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from voice_gateway.conversation_http import TrustedVoiceBinding


class RoutingUnavailable(RuntimeError):
    def __init__(self):
        super().__init__("Voice route unavailable")


class Registration(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    call_id: UUID
    number: str = Field(strict=True, pattern=r"^\+375[0-9]{9}$")


class _Binding(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    number: str
    voice_revision: str
    conversation_revision: str
    owner_id: str = Field(strict=True, min_length=1, max_length=255)
    integration_id: UUID
    agent_id: UUID


def private_url(value: str) -> str:
    url = urlsplit(value)
    if (
        url.scheme not in {"http", "https"}
        or not url.hostname
        or url.username is not None
        or url.password is not None
        or url.path not in {"", "/"}
        or url.query
        or url.fragment
        or not value.isascii()
        or any(ord(c) <= 32 for c in value)
    ):
        raise ValueError("Invalid private service URL")
    return value.rstrip("/")


def service_token(token: SecretStr) -> str:
    raw = token.get_secret_value()
    if not re.fullmatch(r"[A-Za-z0-9._~-]{32,256}", raw):
        raise ValueError("Invalid service credential")
    return raw


def unique_json(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RoutingUnavailable()
        result[key] = value
    return result


class ControlPlaneRoutes:
    def __init__(
        self,
        base_url,
        token,
        *,
        voice_revision,
        conversation_revision,
        allowed_numbers,
        transport=None,
    ):
        if not all(
            re.fullmatch(r"[0-9a-f]{40}", value)
            for value in (voice_revision, conversation_revision)
        ):
            raise ValueError("Invalid source revisions")
        self.allowed_numbers = frozenset(allowed_numbers)
        if (
            not self.allowed_numbers
            or len(self.allowed_numbers) > 50
            or any(not re.fullmatch(r"\+375[0-9]{9}", n) for n in self.allowed_numbers)
        ):
            raise ValueError("An explicit bounded number cohort is required")
        self.voice_revision, self.conversation_revision = voice_revision, conversation_revision
        self.client = httpx.AsyncClient(
            base_url=private_url(base_url),
            headers={"X-Voice-Routing-Token": service_token(token)},
            timeout=3,
            trust_env=False,
            follow_redirects=False,
            transport=transport,
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        )

    async def resolve(self, number):
        try:
            async with asyncio.timeout(3):
                return await self._resolve(number)
        except Exception:
            raise RoutingUnavailable() from None

    async def _resolve(self, number):
        if number not in self.allowed_numbers:
            raise RoutingUnavailable()
        request = {
            "number": number,
            "voice_revision": self.voice_revision,
            "conversation_revision": self.conversation_revision,
        }
        try:
            async with self.client.stream(
                "POST", "/v1/internal/voice/resolve", json=request
            ) as response:
                if (
                    response.status_code != 200
                    or response.headers.get("content-type", "").split(";")[0] != "application/json"
                    or response.headers.get("content-encoding", "identity") != "identity"
                ):
                    raise RoutingUnavailable()
                body = bytearray()
                async for chunk in response.aiter_bytes(8192):
                    body.extend(chunk)
                    if len(body) > 8192:
                        raise RoutingUnavailable()
            binding = _Binding.model_validate(json.loads(body, object_pairs_hook=unique_json))
            if (
                any(getattr(binding, k) != v for k, v in request.items())
                or any(ord(c) < 32 or ord(c) == 127 for c in binding.owner_id)
                or not binding.integration_id.int
                or not binding.agent_id.int
            ):
                raise RoutingUnavailable()
            return TrustedVoiceBinding(binding.owner_id, binding.integration_id, binding.agent_id)
        except Exception:
            raise RoutingUnavailable() from None

    async def close(self):
        await self.client.aclose()


@dataclass
class _Registered:
    number: str
    expires: float
    claimed: bool = False


class TrustedCallRegistry:
    """Volatile, single-use PBX UUID bindings. Never a canonical call/history store."""

    def __init__(self, routes, *, max_entries=128, ttl=30, clock=time.monotonic):
        if not 1 <= max_entries <= 512 or not 1 <= ttl <= 60:
            raise ValueError("Invalid call registry limits")
        self.routes, self.max_entries, self.ttl, self.clock = routes, max_entries, ttl, clock
        self.entries = {}

    def _prune(self):
        now = self.clock()
        self.entries = {key: entry for key, entry in self.entries.items() if entry.expires > now}
        return now

    def register(self, registration: Registration):
        now = self._prune()
        if not registration.call_id.int or registration.number not in self.routes.allowed_numbers:
            raise RoutingUnavailable()
        key = str(registration.call_id)
        old = self.entries.get(key)
        if old:
            if old.claimed or old.number != registration.number:
                raise RoutingUnavailable()
            return  # Replay does not extend the original deadline.
        if len(self.entries) >= self.max_entries:
            raise RoutingUnavailable()
        self.entries[key] = _Registered(registration.number, now + self.ttl)

    async def __call__(self, call_id: str):
        self._prune()
        entry = self.entries.get(call_id)
        if entry is None or entry.claimed:
            raise RoutingUnavailable()
        entry.claimed = True  # Consume before await; concurrent claims cannot both pass.
        return await self.routes.resolve(entry.number)

    async def close(self):
        self.entries.clear()
        await self.routes.close()
