import asyncio
import json
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from pydantic import SecretStr, ValidationError

from voice_gateway.application import VoiceGatewayRuntime
from voice_gateway.live import LiveSettings, build_live_runtime
from voice_gateway.pbx_api import install_pbx_api
from voice_gateway.routing import (
    ControlPlaneRoutes,
    Registration,
    RoutingUnavailable,
    TrustedCallRegistry,
)
from voice_gateway.settings import Settings

NUMBER = "+375170000000"


def routes(handler):
    return ControlPlaneRoutes(
        "http://control-plane.internal",
        SecretStr("a" * 32),
        voice_revision="a" * 40,
        conversation_revision="b" * 40,
        allowed_numbers=[NUMBER],
        transport=httpx.MockTransport(handler),
    )


def good_binding():
    return {
        "number": NUMBER,
        "voice_revision": "a" * 40,
        "conversation_revision": "b" * 40,
        "owner_id": "owner",
        "agent_id": str(uuid4()),
        "integration_id": str(uuid4()),
    }


async def test_control_plane_identity_bound_and_no_caller_selected_agent():
    binding = good_binding()

    def handler(request):
        assert request.url.path == "/v1/internal/voice/resolve"
        assert request.headers["x-voice-routing-token"] == "a" * 32
        assert json.loads(request.content) == {
            k: binding[k] for k in ("number", "voice_revision", "conversation_revision")
        }
        return httpx.Response(200, json=binding)

    client = routes(handler)
    try:
        result = await client.resolve(NUMBER)
        assert str(result.agent_id) == binding["agent_id"]
    finally:
        await client.close()


@pytest.mark.parametrize(
    "fault", ["redirect", "unavailable", "mismatch", "html", "large", "duplicate", "owner", "extra"]
)
async def test_resolution_failure_never_falls_back(fault):
    binding = good_binding()

    def handler(request):
        if fault == "redirect":
            return httpx.Response(302, headers={"location": "http://elsewhere"})
        if fault == "unavailable":
            return httpx.Response(503)
        if fault == "mismatch":
            binding["conversation_revision"] = "c" * 40
        if fault == "owner":
            binding["owner_id"] = "owner\n"
        if fault == "extra":
            binding["secret"] = "never-expose"
        if fault == "html":
            return httpx.Response(200, text="never-expose")
        if fault == "large":
            return httpx.Response(
                200, content=b" " * 8193, headers={"content-type": "application/json"}
            )
        if fault == "duplicate":
            return httpx.Response(
                200,
                content='{"number":"one","number":"two"}',
                headers={"content-type": "application/json"},
            )
        return httpx.Response(200, json=binding)

    client = routes(handler)
    try:
        with pytest.raises(RoutingUnavailable, match="^Voice route unavailable$"):
            await client.resolve(NUMBER)
    finally:
        await client.close()


async def test_registry_single_claim_and_bounded_nonrefreshing_expiry():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json=good_binding())

    client = routes(handler)
    now = [0]
    registry = TrustedCallRegistry(client, max_entries=1, ttl=10, clock=lambda: now[0])
    registration = Registration(call_id=uuid4(), number=NUMBER)
    try:
        registry.register(registration)
        results = await asyncio.gather(
            registry(str(registration.call_id)),
            registry(str(registration.call_id)),
            return_exceptions=True,
        )
        assert sum(isinstance(r, RoutingUnavailable) for r in results) == 1
        assert len(requests) == 1
        with pytest.raises(RoutingUnavailable):
            registry.register(registration)
        with pytest.raises(RoutingUnavailable):
            registry.register(Registration(call_id=uuid4(), number=NUMBER))
        now[0] = 11
        second = Registration(call_id=uuid4(), number=NUMBER)
        registry.register(second)
        now[0] = 20
        registry.register(second)
        now[0] = 22
        with pytest.raises(RoutingUnavailable):
            await registry(str(second.call_id))
        with pytest.raises(RoutingUnavailable):
            registry.register(Registration(call_id=uuid4(), number="+375170000001"))
    finally:
        await registry.close()


async def test_pbx_requires_separate_token_and_rejects_untrusted_identity():
    client = routes(lambda r: httpx.Response(503))
    registry = TrustedCallRegistry(client)
    app = FastAPI()
    install_pbx_api(app, registry, SecretStr("p" * 32))
    registration = {"call_id": str(uuid4()), "number": NUMBER}
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://local"
        ) as api:
            for headers in ({}, {"x-pbx-token": "a" * 32}):
                response = await api.post(
                    "/internal/v1/pbx/calls", headers=headers, json=registration
                )
                assert response.status_code == 401
            headers = {"x-pbx-token": "p" * 32}
            response = await api.post(
                "/internal/v1/pbx/calls",
                headers=headers,
                json={**registration, "agent_id": str(uuid4())},
            )
            assert response.status_code == 422 and NUMBER not in response.text
            response = await api.post("/internal/v1/pbx/calls", headers=headers, json=registration)
            assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
            response = await api.post(
                "/internal/v1/pbx/calls", headers=headers, content=b" " * 4097
            )
            assert response.status_code == 415
            response = await api.post(
                "/internal/v1/pbx/calls",
                headers={**headers, "content-type": "application/json"},
                content=b" " * 4097,
            )
            assert response.status_code == 422
    finally:
        await registry.close()


def config(**changes):
    return LiveSettings(
        **{
            "control_plane_url": "http://control-plane.internal",
            "control_plane_token": "a" * 32,
            "conversation_url": "http://conversation.internal",
            "conversation_token": "b" * 32,
            "pbx_token": "p" * 32,
            "voice_revision": "a" * 40,
            "conversation_revision": "b" * 40,
            "allowed_numbers": NUMBER,
            "stt_provider": "openrouter",
            "stt_api_key": "test-only",
            "stt_model": "test-stt",
            "tts_provider": "openrouter",
            "tts_api_key": "test-only",
            "tts_model": "test-tts",
            "tts_voice": "test",
            **changes,
        }
    )


def test_live_activation_rejects_incomplete_configuration_without_fake_fallback(monkeypatch):
    with pytest.raises(ValueError):
        VoiceGatewayRuntime(Settings(mode="live_audiosocket"))
    with pytest.raises(ValidationError):
        config(tts_api_key="")
    with pytest.raises(ValidationError):
        config(conversation_token="a" * 32)
    with pytest.raises(ValueError):
        build_live_runtime(Settings(mode="live_audiosocket", http_host="0.0.0.0"), config())
    monkeypatch.setattr("voice_gateway.live.shutil.which", lambda name: None)
    with pytest.raises(ValueError, match="ffmpeg"):
        build_live_runtime(Settings(mode="live_audiosocket"), config())


async def test_live_uses_real_audio_adapters_and_closes_clients(monkeypatch):
    from voice_gateway.media.providers.openrouter import OpenRouterSTT, OpenRouterTTS

    monkeypatch.setattr("voice_gateway.live.shutil.which", lambda name: "test-ffmpeg")
    runtime = build_live_runtime(Settings(mode="live_audiosocket"), config())
    assert isinstance(runtime.stt_factory(), OpenRouterSTT)
    assert isinstance(runtime.tts_factory(), OpenRouterTTS)
    await runtime.stop()
    assert runtime.conversation_port.client.is_closed
    assert runtime.registry.routes.client.is_closed


@pytest.mark.parametrize("early_start", [0, 200])
async def test_live_early_stt_is_explicit_and_keeps_confirmed_endpoint(monkeypatch, early_start):
    monkeypatch.setattr("voice_gateway.live.shutil.which", lambda name: "test-ffmpeg")
    runtime = build_live_runtime(
        Settings(mode="live_audiosocket"), config(stt_early_start_ms=early_start)
    )
    try:
        stt = runtime.stt_factory()
        assert stt.queue.early_start_ms == early_start
        assert stt.vad.settings.silence_end_ms == 500
        assert stt.queue.max_pending == 4
    finally:
        await runtime.stop()
    with pytest.raises(ValidationError):
        config(stt_early_start_ms=500)
