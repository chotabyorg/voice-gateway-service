# Donor transplant map

This repository is an independent Main Chota service. It is not a fork or a
renamed copy of the donor.

- Donor: `chotabyorg/chota-va`
- Exact donor `main` SHA inspected: `02266e2c8ad1510c9f78a0e50254f89a3e385b22`
- Reusable-media preparation: donor PR #2, merged as that exact SHA.
- Attribution: code adapted from the donor's `voice_agent/media/` package; this
  file preserves the source revision and component-level mapping.

## Component decisions

| Donor source | Target | Decision | Boundary and reason |
|---|---|---|---|
| `voice_agent/media/interfaces.py` | `voice_gateway/media/interfaces.py` | Adapt | Keep per-call transport/STT/TTS ports; remove the disk-spool port and all platform model claims. |
| `voice_agent/media/audio.py` | `voice_gateway/media/audio.py` | Reuse/adapt | Keep bounded PCM16 LE, 8 kHz, mono helpers and WAV/mix utilities; add explicit byte and duration limits at host boundaries. |
| `voice_agent/media/audiosocket.py` | `voice_gateway/media/audiosocket.py` | Adapt | Keep UUID handshake and bounded AudioSocket framing; add an opt-in listener with call admission limits. No AMI, dialplan, SIP provisioning, or public listener by default. |
| `voice_agent/media/vad.py`, `batch_stt.py` | `voice_gateway/media/` | Reuse/adapt | Keep energy VAD, bounded utterance buffering, bounded STT concurrency/backlog, ordered final callbacks, and cancellation cleanup. |
| `voice_agent/media/providers/groq.py` | `voice_gateway/media/providers/groq.py` | Adapt | Keep the injectable Groq Whisper adapter and explicit settings. No provider client is created unless a host composes it with credentials. |
| `voice_agent/media/providers/edge.py` | `voice_gateway/media/providers/edge.py` | Adapt | Keep the injectable Edge TTS adapter; bound text, streamed MP3 and decoded PCM, and preserve subprocess timeout/cancellation/reaping. Provider calls are not part of local verification. |
| `voice_agent/media/resilience.py` | `voice_gateway/media/resilience.py` | Reuse/adapt | Keep explicit STT start-time fallback and timeout/PCM validation for TTS. Do not add billing, global config, or provider switching policy. |
| `voice_agent/media/interruption.py` | `voice_gateway/media/interruption.py` | Reuse/adapt | Keep sustained-speech barge-in detection and lead-in capture as acoustic behavior only. |
| `voice_agent/media/playback.py`, `speech.py` | `voice_gateway/media/` | Adapt | Keep paced 20 ms PCM output and bounded speech prefetch; emit typed playback facts for started, progress, completed, interrupted, failed, or unknown outcomes. |
| `voice_agent/media/turns.py` | `voice_gateway/media/utterances.py` | Adapt/rename | Keep acoustic utterance assembly and continuation hints. Rename to avoid confusion with Main Conversation `AgentTurn` scheduling. |
| `voice_agent/media/runtime.py`, `completion.py` | `voice_gateway/media/call.py`, `completion.py` | Adapt/rewrite | Keep one-call lifecycle, owned-task draining, graceful cancellation, and resource cleanup. Expose typed local callbacks; no Conversation HTTP client or authority. |
| `voice_agent/media/spool.py`, `AudioSpool` | None | Drop | A process restart ends the live provider stream, so raw-audio recovery cannot resume the call or preserve canonical communication state. The service has no recording or archive requirement. Bounded in-memory frames/utterances are sufficient; avoiding disk audio removes retention, path, and cleanup risk. A boundary test and architecture note will prove no spool exists. |
| `tests/media/` | `tests/media/` and `tests/integration/` | Adapt | Keep tests for PCM, framing, VAD, STT ordering/backpressure, provider contracts with injected fakes, playback, barge-in, and cleanup. Rewrite package imports and remove only spool-specific cases. Add real local TCP/process and container scenarios. |
| `docs/voice/reusable-media.md` | `docs/donor-map.md` plus service docs | Adapt | Use its READY_TO_PORT / NEEDS_ADAPTATION / STANDALONE_ONLY findings as source evidence; do not copy donor product documentation as target architecture. |
| `voice_agent/requirements.txt`, donor Docker/runtime/CI | New service manifests and workflows | Rewrite | Use a Main-Chota-shaped Python 3.11 service, exact direct dependency pins, inert defaults, non-root container, CI, and no donor `.env`, state, telephony, or product runtime. |
| Donor LLM, RAG/knowledge, Bitrix/CRM, ToolAgent, scenarios, tenants, handoff, campaigns/outbound, analytics, call archive, database/jobs, admin API/console, AMI/Twilio/Verto product wiring, PJSUA2 workspace | None | Drop | These are standalone product, AI, business, persistence, or provider-control ownership. They do not belong in Main Voice Gateway. |

## MTS media follow-up (2026-10-13)

Additional donor inspected: `chotabyorg/mts`, `main` commit
`8fd51c236bf43060a788778139a3fbbc4d223be9`.

| Donor source | Target | Decision |
|---|---|---|
| `backend/mts/voice/media/vad.py` | `media/vad.py` | Adapt release-threshold hysteresis; require minimum voiced duration before lowering the threshold. |
| `backend/mts/voice/media/playback.py` | `media/playback.py` | Adapt gain ramps, pause/resume/duck and stop mechanics; preserve bounded pacing and prevent resume after stop. |
| `backend/mts/providers.py` OpenAI-compatible audio adapters and reusable HTTP-client mechanics | `media/providers/openrouter.py` | Rewrite as explicit OpenRouter audio-only adapters with fixed endpoint, documented JSON STT format, bounded streamed MP3 decoding, injected settings and no SDK/runtime routing dependency. |
| Later local MTS snapshots of `providers.py`, `voice/media/playback.py` and `runtime.py` | `media/providers/decoder.py`, `media/playback.py`, `media/call.py` | Adapt incremental audio playback and first-audio/response-gap measurement; isolate counters, revoke late output and bound cancellation. These snapshots are reference material, not claimed to be the donor main revision. |
| SpeechKit/Yandex, ElevenLabs and Silero provider implementations | None | Additional provider activation/adapters are outside this OpenRouter provider transfer. |
| Direct SIP/PJSUA2 session wiring, workspace/Room state, transfer policy, LLM, prompts, CRM, recordings and deployment configuration | None | Do not transplant donor product/runtime ownership into this service. |

The local snapshot identities (SHA-256) are recorded for provenance:

- `tmp/remote_mts/mts/voice/media/runtime.py`:
  `59161e8e20bfa628da17c7ea6966a052efd00f240aa533819a44cef6417b96c3`
- `tmp/remote_mts/mts/voice/media/playback.py`:
  `1f5f96c8061bc1995a3349bf743c6422a8f1d8196876cffb1a7ba12eb3839d56`
- `tmp/silero_remote/providers.py`:
  `b5dbd751143e669b7d17ef5022b39d2fd3a64787b81bc9847ce93ff891045f20`

Those paths identify the inspected donor workspace, not files shipped in this
repository. New tests reproduce AudioSocket identity/closed-write failures,
immediate caller replies, cancellation-resistant TTS, streaming failures and
decoder cleanup. See [adapter behavior and validation](provider-adapters.md).

## Main Chota boundary

The accepted Main Chota Conversation ADR assigns PBX/audio, STT/TTS, VAD,
acoustic utterance coordination, playback, barge-in, and transient call execution
to Voice Gateway. Conversation retains canonical Messages, AgentTurns, authority,
delivery evidence, and linked Call metadata. AI retains reasoning, retrieval, and
Business Runtime work. This goal defines only typed local seams; it does not
activate or call Conversation.

Generated TTS bytes and bytes accepted by the transport are media facts. Neither
proves that a remote caller heard the complete response. Playback completion is
reported only when all frames were handed to the transport; interruptions and
ambiguous transport failures remain distinct outcomes.
