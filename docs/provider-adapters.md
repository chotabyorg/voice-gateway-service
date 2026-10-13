# Optional audio adapters and local media hardening

The service still starts in `disabled` mode. `local_fake` still uses deterministic
fakes. Installing provider dependencies or setting environment variables does
not activate OpenRouter, public telephony or Conversation integration.

## OpenRouter composition

The host must explicitly inject credentials and audio model IDs into
`OpenRouterSTT` and `OpenRouterTTS`. These adapters call only the fixed
`https://openrouter.ai/api/v1/audio/transcriptions` and `/audio/speech` endpoints.
They do not use chat completions, prompts, model routing policy or history.
The host chooses compatible audio models and an optional provider-specific voice;
there is no hardcoded default model, automatic retry or fallback provider.

```python
from voice_gateway.media.providers.openrouter import (
    OpenRouterSTT, OpenRouterSTTSettings, OpenRouterTTS, OpenRouterTTSSettings,
)

# Values come from an explicitly configured host, never repository configuration.
stt = OpenRouterSTT(OpenRouterSTTSettings(api_key=key, model=stt_model, language="ru"))
tts = OpenRouterTTS(OpenRouterTTSSettings(api_key=key, model=tts_model, voice=voice))
# Inject these per-call instances into MediaCall with a transport and local port.
# MediaCall owns stop_stream()/close(); callers using adapters directly must close them.
```

OpenRouter needs `httpx==0.28.1` from `requirements-providers.txt`. TTS also needs
ffmpeg on PATH. Both HTTP clients and the decoder can be injected for testing.
The optional Python dependencies and ffmpeg are not added to the default image.
This snippet is library composition, not a new service mode or an integration
protocol. A real-call host and production activation remain separately reviewed work.

STT is batch HTTP after local VAD/end-of-speech, not streaming recognition. It
encodes canonical PCM16 LE, 8 kHz, mono as an in-memory WAV and sends base64 JSON.
The default batch queue bounds each utterance to 1,000,000 bytes, limits provider
concurrency to 2 and pending work to 8. The HTTP response is limited to 128 KiB
and the transcript to 8,000 characters. The request deadline is 25 seconds.

TTS requests MP3 and incrementally decodes it to canonical PCM with ffmpeg. It
can start playback before the HTTP response ends; actual first-audio latency also
depends on model generation, provider flushing and decoder buffering. It does
not assume a provider's raw PCM sample rate. Defaults: 2,000 text characters,
3,000,000 encoded bytes, 1,920,000 PCM bytes (120 seconds), 25-second HTTP I/O
timeout and 120-second whole-stream deadline. Playback accepts at most 64 KiB
per chunk and paces output as 20 ms frames, including an aligned final short frame.
The encoded stream, decoder pipes and HTTP stream are closed on failure,
cancellation or early exit. Audio is never written to disk.

Connections are reused within each adapter instance (two connections, 120-second
keepalive). Redirects and environment proxy discovery are disabled. Invalid MIME,
malformed JSON, oversized data and HTTP errors fail explicitly. Error messages
expose status/type only; provider response bodies, credentials and transcript
content are not logged. API keys are omitted from settings representations.

Wire formats follow the official [STT](https://openrouter.ai/docs/guides/overview/multimodal/stt)
and [TTS](https://openrouter.ai/docs/guides/overview/multimodal/tts) documentation,
checked on 2026-10-13. Fake HTTP tests verify request shape, bounds, closure and
failure behavior; they do not establish a particular model's availability or quality.

## Local media changes

- AudioSocket rejects writes after close, identity changes after the UUID
  handshake and explicit peer error frames. An identical repeated UUID is
  idempotent. Existing EOF, idle and framing bounds remain in place.
- VAD uses an adjustable release threshold (default 60% of the onset threshold)
  after the minimum voiced duration. This retains quieter speech endings without
  promoting a short click. Batch STT processes input in 20 ms slices.
- Playback supports streamed PCM, split network samples and local gain ramps.
  Slow transport writes or provider stalls reset pacing rather than cause a burst.
  Gain pause/resume/duck mechanics are local; they introduce no platform commands.
- Barge-in can fade output over 40 ms, waiting up to 60 ms for that fade before
  revocation. Provider cancellation and transport clear each have a default
  200 ms deadline. These are sequential budgets, not a single 200 ms SLA.
  A provider that ignores cancellation cannot write late audio or admit another
  concurrent output on the same call. Its pending task is observed until settled.
  New playback waits for the previous interrupt's transport clear; if clear
  ignores cancellation, new output is rejected until that operation settles.
  A late clear therefore cannot erase a subsequent response.
- Barge-in analyzes fixed 20 ms PCM windows across PBX packet boundaries. It
  retains a partial window until more samples arrive, rather than averaging a
  whole packet's silence and speech. Detection is quantized to 20 ms. On an
  interruption, both the acoustic preroll and the remainder of the triggering
  packet reach STT. Guard/calibration timing uses each frame's estimated end
  relative to the packet's arrival; it is not a remote playback timestamp.
- Default post-playback input suppression is now zero, preserving an immediate
  caller reply. Hosts can explicitly configure acoustic suppression if measured
  echo conditions require it; this is not an echo-cancellation algorithm.

The local playback dataclasses and enums are unchanged. Counters are isolated
per output. In-flight transport writes remain `unknown` when cancelled; completed
generation is never reported as transport completion. Barge-in cannot retract
frames already accepted by an AudioSocket peer or prove what the caller heard.

## Timing and verification

Structured events contain counts, durations and error types:

- `stt_request_timing`: provider duration and queue time.
- `media_first_audio`: synthesis start to first successful transport write.
- `media_response_gap`: last locally observed voiced frame for that utterance to
  first successful transport write; includes endpointing, STT and local dispatch.
- `media_interrupt_timing`: local interrupt work, including bounded cancellation.
- `playback_pacing_stall`: scheduling/write/provider delay in milliseconds;
  this is not a measurement of RTP network jitter.

Speech timestamps travel through private local media state only. They are not
new fields in `VoiceToConversationPort` or a production wire contract.

Tests use fake providers, real local subprocesses and loopback AudioSocket calls.
When ffmpeg is available, an additional local test encodes a synthetic tone as
24 kHz stereo MP3 and verifies decoding into 8 kHz mono PCM. Tests cover early
playback, interruption, non-cooperative cancellation, source failure, subprocess
reaping, malformed audio and size/time limits. No paid API requests or real
telephone calls are made. Russian recognition accuracy, voice naturalness,
real-world latency and PBX echo remain live acceptance work.
