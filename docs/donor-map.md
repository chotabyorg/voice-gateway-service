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
