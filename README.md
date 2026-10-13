# Voice Gateway Service

Independent Main Chota service foundation for transient telephony and media
execution. It adapts bounded audio transport, VAD/end-of-speech, STT/TTS,
playback and barge-in. It does not own canonical conversation history, AI or
Business behavior.

## Safe local startup

Python 3.11 is required. The app is inert by default and does not bind an
AudioSocket listener unless explicitly enabled.

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
python -m voice_gateway
```

`GET /health` returns liveness. `GET /ready` returns `503` in the default
disabled mode and `200` only when the explicitly enabled local-fake listener is
ready. API docs and debug routes are disabled.

For the disposable deterministic fake stack:

```sh
VOICE_MODE=local_fake docker compose up --build
```

The sample Compose file publishes only to `127.0.0.1`, uses a dedicated Docker
bridge, a read-only root filesystem and UID 10001. This fake-only composition
does not configure outbound provider clients. The CI container checks use an
internal no-egress network. A client that stops sending AudioSocket frames is
closed after `VOICE_AUDIOSOCKET_IDLE_TIMEOUT_SECONDS` and releases its call
slot. Send an AudioSocket UUID handshake followed by PCM16 LE, 8 kHz mono audio
to `127.0.0.1:55585` to exercise the local media path.

## Development checks

```sh
pip install -r requirements-dev.txt
ruff check .
ruff format --check .
mypy
pytest -q
```

The process integration tests use a real local TCP socket and OS process with
deterministic fakes. Container CI additionally checks the built image, fake call,
non-root runtime and graceful SIGTERM shutdown.

For native media acceptance without Docker, credentials or external services:

```sh
python -m scripts.media_soak --rounds 120 --concurrency 8 --output media-soak.json
```

The script binds only an ephemeral loopback port, uses synthetic PCM/providers,
checks call/resource cleanup and writes metadata-only latency/memory statistics.
See [`docs/media-validation.md`](docs/media-validation.md) for reproducible checks,
the recorded local result and the boundary between local and live acceptance.

## Optional provider adapters

`voice_gateway/media/providers/` contains injectable OpenRouter STT/streaming
TTS, Groq Whisper STT and Edge TTS adapters. Dependencies are optional
(`requirements-providers.txt`); the application does not instantiate them.
OpenRouter and Edge TTS require an external ffmpeg decoder, which is not included
in the default container. No real provider key is needed for the tests.

See [`docs/provider-adapters.md`](docs/provider-adapters.md) for explicit
composition, bounds, cancellation behavior and the limits of local validation.

## Architecture and handoff

- [`docs/architecture.md`](docs/architecture.md): ownership and local
  Conversation seam.
- [`docs/donor-map.md`](docs/donor-map.md): exact donor provenance and component
  decisions.
- [`docs/artem-handoff.md`](docs/artem-handoff.md): Voice team scope and stop
  conditions.
- [`docs/telephony-onboarding.md`](docs/telephony-onboarding.md): confirmed shared
  ingress target, reservation/activation distinction and the pending contract gate.
- [`docs/deployment-contract.md`](docs/deployment-contract.md): future
  deployment constraints only; no deployment resources.
- [`AGENTS.md`](AGENTS.md): instructions future coding agents must follow.
