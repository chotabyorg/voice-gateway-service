# Speech adapter contract

All call-side ports use `PCMFormat()` (signed little-endian PCM16, 8 kHz, mono).
Capabilities declare native stream/batch behavior, partial/final results,
local monotonic acoustic timestamps, cancellation and output chunk bounds.
Batch STT owns its bounded ordered queue and VAD; provider timestamps are not
copied into acoustic timing. MediaCall requires final STT and cancellable ports.
Batch TTS has an explicit bounded chunk view. Native streaming is declared by
the adapter, including through the optional greeting wrapper.

Existing cloud defaults remain unchanged. `tts_pronounce_booking_values=None`
retains full Russian booking pronunciation for SpeechKit and opt-in behavior
for other providers. `tts_pronounce_booking_values=true` and `tts_expand_byn=true`
apply the same shared pronunciation function exactly once per speech fragment.
Canonical Conversation text is never rewritten.

The optional `pcm_http` adapter targets an explicitly configured private IP or
localhost endpoint. It loads no models and introduces no inference workers.
Configure `VOICE_LIVE_STT_BASE_URL` / `VOICE_LIVE_TTS_BASE_URL`, provider/model
and optional `STT_ENDPOINT_API_KEY` / `TTS_ENDPOINT_API_KEY` under the same
`VOICE_LIVE_` prefix. Cloud API keys are never reused for this endpoint.
The three control-plane, Conversation and PBX credentials remain mandatory
and distinct.

STT posts multipart WAV to `/audio/transcriptions` and expects a bounded JSON
object with a `text` string. TTS posts model, input, voice, `response_format=pcm`
and `sample_rate` to `/audio/speech`. It requires exactly
`Content-Type: audio/pcm;rate=<configured rate>;channels=1`, without compression.
`VOICE_LIVE_TTS_FORMAT=pcm` and `TTS_PCM_SAMPLE_RATE` are explicit. No retries,
redirect following, provider fallback or router fields are added.

8 kHz PCM passes through without conversion; 24 kHz uses the existing bounded
native FIR converter. 16/48 kHz require the existing ffmpeg streaming decoder,
validated only when selected. Every stream has a shared deadline and bounded
input/output. Its owner closes the HTTP response, converter and per-call client
on completion, error or cancellation. Double close is safe.

Provider selection is still explicit and inert by default. Native/fake contract
success proves interchangeability of the port, not the quality, capacity or
economics of a future inference engine. Qualify that engine separately before
changing the production provider configuration.
