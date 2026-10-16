"""Explicit real-provider composition. No direct AI client or fallback providers."""

import asyncio
import importlib.util
import ipaddress
import shutil
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from voice_gateway.application import VoiceGatewayRuntime
from voice_gateway.conversation_http import ConversationHTTPPort
from voice_gateway.media.batch_stt import BatchSTTSettings
from voice_gateway.media.providers.edge import EdgeTTS, EdgeTTSSettings
from voice_gateway.media.providers.groq import GroqSTT, GroqSTTSettings
from voice_gateway.media.providers.openrouter import (
    OpenRouterSTT,
    OpenRouterSTTSettings,
    OpenRouterTTS,
    OpenRouterTTSSettings,
)
from voice_gateway.media.vad import VADSettings
from voice_gateway.routing import (
    ControlPlaneRoutes,
    TrustedCallRegistry,
    private_url,
    service_token,
)


class LiveSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="VOICE_LIVE_", extra="forbid", hide_input_in_errors=True
    )
    control_plane_url: str
    control_plane_token: SecretStr
    conversation_url: str
    conversation_token: SecretStr
    pbx_token: SecretStr
    voice_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    conversation_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    allowed_numbers: str
    stt_provider: Literal["groq", "openrouter"]
    stt_api_key: SecretStr
    stt_model: str = Field(min_length=1, max_length=200)
    stt_language: Literal["", "ru", "en", "be"] = "ru"
    stt_early_start_ms: int = Field(default=0, ge=0, lt=500)
    vad: Literal["energy", "webrtc", "ten"] = "energy"
    tts_provider: Literal["edge", "openrouter"]
    tts_api_key: SecretStr = SecretStr("")
    tts_model: str = Field(default="", max_length=200)
    tts_voice: str = Field(min_length=1, max_length=200)
    tts_format: Literal["mp3", "pcm"] = "mp3"
    tts_expand_byn: bool = False

    @model_validator(mode="after")
    def validate_activation(self):
        for token in (self.control_plane_token, self.conversation_token, self.pbx_token):
            service_token(token)
        if (
            len(
                {
                    t.get_secret_value()
                    for t in (self.control_plane_token, self.conversation_token, self.pbx_token)
                }
            )
            != 3
        ):
            raise ValueError("Separate boundary credentials are required")
        private_url(self.control_plane_url)
        private_url(self.conversation_url)
        if not self.stt_api_key.get_secret_value():
            raise ValueError("STT credential is required")
        if self.tts_provider == "openrouter" and (
            not self.tts_api_key.get_secret_value() or not self.tts_model
        ):
            raise ValueError("OpenRouter TTS model and credential are required")
        return self


class LiveRuntime(VoiceGatewayRuntime):
    sip_client = None

    @property
    def ready(self):
        if self.settings.mode == "live_sip":
            return self.sip_client is not None and self.sip_client.registered and not self._closing
        return super().ready

    async def start(self):
        if self.settings.mode != "live_sip":
            return await super().start()
        from voice_gateway.sip import SIPClient, SIPSettings

        settings = SIPSettings()
        if settings.max_calls > self.settings.max_calls:
            raise ValueError("SIP admission cannot exceed runtime capacity")
        self.sip_client = SIPClient(settings, self.handle_transport, self.registry)
        self._closing = False
        await self.sip_client.start()

    async def stop(self):
        try:
            await super().stop()
        finally:
            try:
                if self.sip_client is not None:
                    await self.sip_client.stop()
            finally:
                await asyncio.gather(self.conversation_port.close(), self.registry.close())


def build_live_runtime(settings, config=None):
    config = config or LiveSettings()
    # AudioSocket has no peer authentication; this mode requires a local trusted SIP/PBX adapter.
    if not all(
        ipaddress.ip_address(host).is_loopback
        for host in (settings.http_host, settings.audiosocket_host)
    ):
        raise ValueError("Live PBX ingress must bind to loopback")
    if shutil.which("ffmpeg") is None:
        raise ValueError("Real TTS requires ffmpeg")
    for module in (
        "groq" if config.stt_provider == "groq" else "httpx",
        "edge_tts" if config.tts_provider == "edge" else "httpx",
    ):
        if importlib.util.find_spec(module) is None:
            raise ValueError("Selected speech provider dependency is unavailable")
    if settings.mode == "live_sip" and importlib.util.find_spec("pjsua2") is None:
        raise ValueError("Native PJSUA2 is required for live SIP")
    classifier_factory = None
    if config.vad == "webrtc":
        if importlib.util.find_spec("webrtcvad") is None:
            raise ValueError("Selected WebRTC speech classifier is unavailable")
        from voice_gateway.speech_activity import WebRTCSpeech

        classifier_factory = WebRTCSpeech
    elif config.vad == "ten":
        if importlib.util.find_spec("ten_vad") is None:
            raise ValueError("Selected TEN speech classifier is unavailable")
        from voice_gateway.speech_activity import TenSpeech

        # Load and exercise the native library before accepting any calls.
        # A missing libc++/DLL must fail startup, not the first live call.
        TenSpeech()(bytes(320))
        classifier_factory = TenSpeech

    queue = BatchSTTSettings(max_pending=4, early_start_ms=config.stt_early_start_ms)
    vad = VADSettings(max_utterance_ms=30000, min_speech_ms=120 if classifier_factory else 80)
    if config.stt_provider == "groq":
        stt_config = GroqSTTSettings(
            api_key=config.stt_api_key.get_secret_value(),
            model=config.stt_model,
            language=config.stt_language,
        )

        def stt_factory():
            return GroqSTT(
                stt_config,
                vad=vad,
                queue=queue,
                speech_classifier=classifier_factory() if classifier_factory else None,
            )
    else:
        stt_config = OpenRouterSTTSettings(
            api_key=config.stt_api_key.get_secret_value(),
            model=config.stt_model,
            language=config.stt_language,
        )

        def stt_factory():
            return OpenRouterSTT(
                stt_config,
                vad=vad,
                queue=queue,
                speech_classifier=classifier_factory() if classifier_factory else None,
            )

    if config.tts_provider == "edge":
        tts_config = EdgeTTSSettings(voice=config.tts_voice)

        def tts_factory():
            return EdgeTTS(tts_config)
    else:
        tts_config = OpenRouterTTSSettings(
            api_key=config.tts_api_key.get_secret_value(),
            model=config.tts_model,
            voice=config.tts_voice,
            response_format=config.tts_format,
            expand_byn=config.tts_expand_byn,
        )

        def tts_factory():
            return OpenRouterTTS(tts_config)

    routes = ControlPlaneRoutes(
        config.control_plane_url,
        config.control_plane_token,
        voice_revision=config.voice_revision,
        conversation_revision=config.conversation_revision,
        allowed_numbers=config.allowed_numbers.split(","),
    )
    registry = TrustedCallRegistry(routes)
    conversation = ConversationHTTPPort(
        config.conversation_url, config.conversation_token, registry
    )
    runtime = LiveRuntime(
        settings,
        conversation_port=conversation,
        stt_factory=stt_factory,
        tts_factory=tts_factory,
        speech_classifier_factory=classifier_factory,
    )
    runtime.registry = registry
    runtime.pbx_token = config.pbx_token
    return runtime
