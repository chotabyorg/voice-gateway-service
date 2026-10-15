"""Typed, inert-by-default runtime configuration."""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VOICE_", extra="ignore")

    mode: Literal["disabled", "local_fake", "live_audiosocket", "live_sip"] = "disabled"
    http_host: str = "127.0.0.1"
    http_port: int = Field(default=8000, ge=1, le=65535)
    audiosocket_host: str = "127.0.0.1"
    audiosocket_port: int = Field(default=55585, ge=0, le=65535)
    audiosocket_idle_timeout_seconds: float = Field(default=30, gt=0, le=300)
    max_calls: int = Field(default=8, ge=1, le=128)
    max_frame_bytes: int = Field(default=4096, ge=2, le=65535)
    max_call_seconds: float = Field(default=3600, gt=0, le=86400)
    shutdown_timeout_seconds: float = Field(default=8, gt=0, le=60)
