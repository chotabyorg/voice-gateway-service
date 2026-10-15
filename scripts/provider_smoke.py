"""Opt-in synthetic speech probe using an operator-selected existing MTS profile.

Only OpenRouter audio endpoints are used. Credentials stay in process memory;
the supplied phrase, audio and transcript are never written or printed.
"""

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path

from dotenv import dotenv_values

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from voice_gateway.media.providers.openrouter import (  # noqa: E402
    BASE_URL,
    OpenRouterSTT,
    OpenRouterSTTSettings,
    OpenRouterTTS,
    OpenRouterTTSSettings,
)

PHRASE = "Здравствуйте. Это проверка голосовой связи. Запишите меня на завтра в десять часов."


def word_errors(reference, hypothesis):
    expected = re.findall(r"\w+", reference.lower())
    actual = re.findall(r"\w+", hypothesis.lower())
    row = list(range(len(actual) + 1))
    for i, left in enumerate(expected, 1):
        previous, row = row, [i]
        for j, right in enumerate(actual, 1):
            row.append(min(previous[j] + 1, row[-1] + 1, previous[j - 1] + (left != right)))
    return row[-1], len(expected)


async def main(path):
    config = dotenv_values(path, interpolate=False)
    if any(
        (config.get(key) or "").rstrip("/") != BASE_URL for key in ("STT_BASE_URL", "TTS_BASE_URL")
    ):
        raise ValueError("Profile must explicitly select the supported OpenRouter audio API")
    stt = OpenRouterSTT(
        OpenRouterSTTSettings(config["STT_API_KEY"], config["STT_MODEL"], language="ru")
    )
    tts = OpenRouterTTS(
        OpenRouterTTSSettings(config["TTS_API_KEY"], config["TTS_MODEL"], voice=config["TTS_VOICE"])
    )
    stage, report = "tts", {"synthetic_speech_probe": "failed"}
    try:
        started, first = time.monotonic(), None
        audio = bytearray()
        async for chunk in tts.synthesize_stream(PHRASE):
            if first is None:
                first = time.monotonic()
            audio.extend(chunk)
        assert first is not None and audio, "No generated PCM"
        report.update(
            {
                "tts_first_pcm_ms": round((first - started) * 1000),
                "tts_total_ms": round((time.monotonic() - started) * 1000),
                "audio_seconds": round(len(audio) / 16000, 2),
            }
        )
        stage, started = "stt", time.monotonic()
        transcript = await stt._transcribe(bytes(audio))
        errors, words = word_errors(PHRASE, transcript)
        report.update(
            {
                "stt_ms": round((time.monotonic() - started) * 1000),
                "word_errors": errors,
                "reference_words": words,
                "transcript_nonempty": bool(transcript),
            }
        )
        assert transcript and errors / words < 0.5, "Synthetic phrase mismatch"
        report["synthetic_speech_probe"] = "passed"
    except Exception as error:
        report.update({"failed_stage": stage, "error_type": type(error).__name__})
        status = getattr(error, "status", None)
        if isinstance(status, int):
            report["http_status"] = status
        print(json.dumps(report))
        return 1
    finally:
        await asyncio.gather(stt._close(), tts.close())
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, required=True)
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.env_file)))
