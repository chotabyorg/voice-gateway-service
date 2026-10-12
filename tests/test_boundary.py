"""Small guardrails against product/AI ownership leaking into this runtime."""

import ast
import logging
from pathlib import Path

from voice_gateway.logging_config import JsonFormatter
from voice_gateway.settings import Settings


def test_runtime_imports_and_requirements_stay_outside_prohibited_ownership():
    root = Path(__file__).resolve().parents[1]
    forbidden = {
        "voice_agent",
        "chota_va",
        "redis",
        "sqlite3",
        "openai",
        "litellm",
        "langchain",
        "chromadb",
        "bitrix",
        "toolagent",
        "businessoperation",
        "campaigns",
        "crm",
    }
    for path in (root / "voice_gateway").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            module = node.module if isinstance(node, ast.ImportFrom) else None
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            else:
                modules = [module] if module else []
            assert all(name.split(".")[0].casefold() not in forbidden for name in modules), path
    dependency_files = [root / "requirements.txt", root / "requirements-providers.txt"]
    requirement_text = "\n".join(
        path.read_text(encoding="utf-8").casefold() for path in dependency_files
    )
    assert not any(name in requirement_text for name in forbidden)


def test_default_configuration_is_inert_without_provider_credentials(monkeypatch):
    monkeypatch.delenv("VOICE_MODE", raising=False)
    settings = Settings(_env_file=None)
    assert settings.mode == "disabled"
    assert not hasattr(settings, "groq_api_key") and not hasattr(settings, "edge_tts_key")


def test_json_log_formatter_excludes_transcripts_phone_numbers_and_secrets():
    record = logging.LogRecord("voice", logging.INFO, "", 0, "stt_final", (), None)
    record.text = "sensitive transcript"
    record.phone_number = "+15551234567"
    record.api_key = "secret-value"
    rendered = JsonFormatter().format(record)
    assert "sensitive transcript" not in rendered
    assert "+15551234567" not in rendered
    assert "secret-value" not in rendered
