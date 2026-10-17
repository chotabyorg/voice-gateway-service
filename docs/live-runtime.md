# Explicit provider runtime

`VOICE_MODE=live_audiosocket` composes real STT/TTS, trusted DID lookup in Control
Plane and the candidate Conversation HTTP port. Default mode remains disabled.
No direct AI call, fake fallback, canonical history or recording archive is added.
The HTTP and AudioSocket listeners must bind loopback. A trusted local SIP/PBX
adapter is required; an open AudioSocket port is not an authenticated PBX ingress.

Install the pinned service and provider requirements plus native `ffmpeg`.
Configuration uses `VOICE_LIVE_` with these required suffixes:

- `CONTROL_PLANE_URL`, `CONTROL_PLANE_TOKEN`
- `CONVERSATION_URL`, `CONVERSATION_TOKEN`
- `PBX_TOKEN` (separate from both service credentials)
- `VOICE_REVISION`, `CONVERSATION_REVISION` (exact source SHA1s)
- `ALLOWED_NUMBERS` (explicit comma-separated Belarus DID cohort)
- `STT_PROVIDER` (`groq`, `openrouter` or `speechkit`), `STT_API_KEY`, `STT_MODEL`
- `TTS_PROVIDER` (`edge`, `openrouter` or `speechkit`), `TTS_VOICE`
- For OpenRouter TTS: `TTS_API_KEY`, `TTS_MODEL`

Optional `VOICE_LIVE_TTS_FORMAT=pcm` selects raw PCM streaming for OpenRouter
instead of the default MP3. Only a single `audio/pcm` Content-Type declaring
`rate=24000;channels=1` is accepted. The bounded decoder explicitly consumes
PCM16 little-endian and resamples to canonical 8 kHz mono, without MP3 format
probing; output is flushed as it becomes available. Ambiguous format metadata,
truncated samples, byte limits and timeouts fail explicitly. Cancellation still
closes the provider stream and reaps the decoder. This option does not change
the TTS model or voice, call authority, playback facts, or text channels.

Optional `STT_LANGUAGE` defaults to `ru`. Credentials belong in a protected
service environment, never source or browser settings. Selected provider
dependencies and ffmpeg are required before the listener starts. Each call owns
its speech clients; runtime shutdown drains calls then closes boundary clients.

Optional `VOICE_LIVE_STT_EARLY_START_MS=200` overlaps batch recognition with
the existing 500 ms acoustic endpoint wait. It defaults to `0` (disabled).
After enough classified speech and 200 ms silence, one bounded request uses a
snapshot of the buffered audio. No transcript is delivered until the normal
endpoint is confirmed. If speech resumes, the early request is cancelled and
its result fenced; final recognition uses the complete original utterance.
Only one early attempt is allowed per utterance, so resumed speech can incur
at most one additional provider request. Provider concurrency and backlog
limits still apply, and early work is skipped when capacity is unavailable.
Empty/error responses are not automatically retried. Call hangup cancels early
work and fences even cancellation-resistant providers; explicit offline drain
can confirm the buffered utterance. This does not change Conversation scheduling,
text settling, final admission, or AI/playback authorization.

`stt_early_started`, `stt_early_discarded` and `stt_early_confirmed` contain
metadata only. The confirmed event's `duration_ms` measures overlapped waiting,
not guaranteed end-to-end savings. With a 200 ms start and 500 ms endpoint,
up to 300 ms of provider work can overlap; network/model/TTS latency remains.

### SpeechKit v3 streaming (opt-in)

Install `requirements-speechkit.txt` in addition to the base runtime. Choose
STT and TTS independently; neither replaces an existing provider automatically.
An active Yandex Cloud folder and least-privilege service-account API key are
required. Use `ai.speechkit-stt.user` / `ai.speechkit-tts.user` roles and matching
`yc.ai.speechkitStt.execute` / `yc.ai.speechkitTts.execute` key scopes. Store the
credential only in the protected service environment.

STT: `STT_PROVIDER=speechkit`, `STT_MODEL=general`, `STT_LANGUAGE=ru`,
`STT_EARLY_START_MS=0`. This uses API v3 REAL_TIME recognition with PCM16 mono
8 kHz, ru-RU whitelist, disabled text refinement and external EOU. Audio starts
streaming after minimum local speech confirmation, including bounded preroll.
The existing local 500 ms endpoint sends EOU; partials never enter Conversation.
One TLS channel is reused per call, with at most four pending utterances and
1600 queued 20 ms frames per RPC (a full 30 s utterance plus margin, so
continuous speech survives a transiently slow stream; a stuck RPC still
fails through its own timeouts and emits `stt_audio_backlog` with the
observed depth). RPC failures, queue overflow or missing final
completion fail explicitly; there are no provider retries or silent fallback.
Hangup cancels work without flushing unfinished speech. `stt_request_timing`
measures waiting after the confirmed endpoint, not total RPC time.

TTS: `TTS_PROVIDER=speechkit`, `TTS_MODEL=livetts`, `TTS_VOICE=sofia`,
`TTS_ROLE=support`, `TTS_FORMAT=pcm`. Supported candidate combinations are
LiveTTS sofia/vera with support/casual, and general marina with
neutral/friendly/whisper. `TTS_EXPAND_BYN=true` retains natural Belarusian
currency pronunciation. LiveTTS uses LUFS without speed/pitch overrides.
`StreamSynthesis` emits telephone PCM directly, without ffmpeg or resampling.
Separate text-only messages are accepted; they are not completion or playback
evidence. Audio length, message size and duration are bounded. Barge-in closes
the RPC; incomplete audio or a midstream error stays a failed/partial playback.
Only fully authorized Conversation speech is submitted, never model drafts.

Both adapters send `x-data-logging-enabled: false`, log metadata only and retain
no audio files. Belarus PBX routing, DID validation, Conversation authority,
Gemma configuration and all text-channel queues remain unchanged.

Current status and measurements: [SpeechKit validation](speechkit-validation.md).

### Optional native OpenRouter PCM conversion

`TTS_NATIVE_PCM_RESAMPLER=true` with OpenRouter `TTS_FORMAT=pcm` selects a bounded
241-tap FIR converter from 24 to 8 kHz. Default remains the existing ffmpeg path.
The filter preserves the telephone band and attenuates aliases above 4 kHz;
it adds 5 ms lookahead and preserves the final sample duration by edge padding.
On three matched synthetic Voice-host trials, first audio improved only 11–17 ms.
This is not a solution to multi-second STT/AI latency. It has not been activated
on the live pilot; SpeechKit does not use this converter.

Optional `VOICE_LIVE_VAD=webrtc` selects a local WebRTC speech classifier instead
of the default energy threshold. It operates on 20 ms, 8 kHz PCM frames and
requires the pinned optional `webrtcvad-wheels` dependency. STT segmentation and
the acoustic-final coordinator use the same speech activity clock, so background
energy cannot keep postponing an already classified final. Playback interruption
uses a separate per-call classifier plus its existing energy/duration guards.
Preroll and the complete buffered utterance are preserved; classification does
not remove samples from recognized speech. This is acoustic endpointing, not
Conversation text settling or AgentTurn scheduling.

`VOICE_LIVE_VAD=ten` explicitly selects TEN VAD, pinned to upstream commit
`d68ab71c8d5fbd8f98e97ae7c9986af76b5d72b1`. Install `requirements-ten-vad.txt`
and, on Linux x64, `libc++1`. Windows x64 is supported by the upstream DLL.
The native library is loaded and exercised before accepting calls. No cloud
request, recording, new model billing or automatic fallback is involved.

Each call has separate STT and interruption classifier state. Only the detector
input is continuously interpolated from 8 to 16 kHz, in two 10 ms hops per
canonical frame, with threshold 0.5. STT keeps the original 8 kHz PCM, 200 ms
preroll and 500 ms trailing silence. Barge-in retains its duration/energy guards
and feeds even quiet frames to the classifier. Default energy mode and optional
WebRTC remain unchanged. This does not change Conversation or text settling.

Upstream's [license](https://github.com/TEN-framework/ten-vad/blob/d68ab71c8d5fbd8f98e97ae7c9986af76b5d72b1/LICENSE)
is Apache 2.0 with additional Agora conditions, including restrictions on
competing offerings; it is not the unmodified Apache license.

`stt_endpoint` records buffered audio, classified speech and trailing silence
durations plus the boundary reason (`silence`, `limit`, or explicit `flush`).
It contains no audio or transcript. Combined with STT/AI/first-audio timestamps,
this separates acoustic waiting from provider and playback latency.

The PBX adapter authenticates to `POST /internal/v1/pbx/calls` with `X-PBX-Token`
and JSON `{ "call_id": "<UUID>", "number": "+375…" }`. It supplies a new UUID
and the trusted called DID. Owner and agent fields are rejected. Registration is
bounded to 128 entries, 30 seconds and 4 KiB requests; retries do not extend TTL.
The matching AudioSocket UUID consumes its registration before the Control Plane
lookup. The platform must return the exact DID and source pair. Unknown calls,
duplicate claims, invalid responses and unavailable dependencies fail closed.

Registration is not proof of an active integration. `/ready` proves configured
local listener availability only, not remote routing/provider quality or completed
real-call acceptance. The SIP adapter, the actual PBX DID provenance, deployed
Conversation/worker grants and routing evidence must be established before
switching the test number. This source change does not switch any live route.

Only `+375173820015` is in the controlled acceptance scope. Other pool numbers
stay inactive. Shared DID routing must not infer the called number from caller ID
or silently map an unknown extension to the test number.

## Native inbound SIP

`VOICE_MODE=live_sip` uses the same providers and Conversation boundary through
PJSUA2, with no AudioSocket listener or PBX registration HTTP endpoint. Install
the native binding for the service Python interpreter. This host adapter lives
outside the portable, standard-library-only `media` package.

Supply `VOICE_SIP_SERVER` (trusted PBX IPv4 address), `DOMAIN`, `USERNAME`,
`PASSWORD` and `BIND_ADDRESS`. Optional suffixes are `PORT` (5060), `SERVER_PORT`
(5060), `RTP_PORT` (14000, even), `MAX_CALLS` (2, no more than runtime capacity),
`REGISTRATION_TIMEOUT` (15 seconds) and `MEDIA_IDLE_TIMEOUT` (30 seconds).
These suffixes all use the `VOICE_SIP_` prefix. Credentials must stay in the
protected service environment. Only PCMA/PCMU at 8 kHz are enabled.

`VOICE_SIP_INPUT_GAIN` is an optional integer multiplier from 1 to 32 (default 1).
Use it only after measuring an unusually quiet carrier input. It scales inbound
PCM before VAD, STT and barge-in detection, preserves digital silence, and clips
at PCM16 limits instead of wrapping. It never changes outbound speech. Fixed
gain also raises background noise; it is not noise removal or automatic gain
control. Verify quiet speech, idle noise and interruption on the selected line.
Each call logs only input byte count, maximum frame RMS before gain and configured
gain. Empty STT responses have a separate event; audio and transcripts stay out
of logs. These diagnostics do not establish successful transcription or playback.

The default DID source is the INVITE request URI from the configured PBX peer.
`VOICE_SIP_CALLED_PARTY_HEADER=P-Called-Party-ID` or `X-Called-Number` is allowed
only after proving the PBX overwrites that header with its trusted called DID.
Missing, duplicated or unknown DID values fail closed. Caller ID and a fixed
fallback number are never used. Each accepted inbound call generates a fresh
UUID and consumes the same bounded trusted registration and platform lookup.
Outbound calls, REFER transfers and Replaces are unsupported.

Readiness in this mode requires SIP registration. It does not prove carrier DID
provenance, RTP reachability, STT/TTS quality or Conversation acceptance. Native
loopback verification and the one-number end-to-end acceptance remain separate
from tests using a PJSUA2 double. No live carrier route is changed by choosing a
mode in source code.

On hangup, media cleanup aborts batch STT: it drops unsubmitted audio, cancels
queued and in-flight requests and fences late results. Provider cancellation and
close share a deadline of at most 0.5 seconds. This avoids retaining the single
call slot for the normal five-second STT drain while a caller redials. Explicit
offline `stop_stream()` still drains results; admission limits remain unchanged.
### Russian BYN pronunciation

For an explicitly selected OpenRouter TTS route, `VOICE_LIVE_TTS_EXPAND_BYN=true`
expands BYN amounts only in the text sent to speech synthesis: `50 BYN` becomes
`50 белорусских рублей`, and `49,90 BYN` becomes
`49 белорусских рублей 90 копеек`. Integer amounts use Russian ruble endings.
Canonical replies, CRM values, other currencies and playback authority are
unchanged. The option is disabled by default; both input and expanded text must
fit the existing text bound.
