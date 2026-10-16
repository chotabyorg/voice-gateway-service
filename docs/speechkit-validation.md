# SpeechKit streaming candidate — 2026-10-16

This is source/native-test evidence, not live pilot acceptance or a latency claim.
User authorized SpeechKit STT/TTS for the single controlled Belarus pilot and
required retaining Gemma, leaving text queues unchanged and using no Docker.

## Native checks

`ruff check .`, `ruff format --check .`, configured strict `mypy`, and the complete
native `pytest -q` suite passed: **296 tests**. Tests include a real loopback
gRPC server using the pinned official Yandex protobuf/service definitions.

Verified: audio reaches STT while speaking; preroll and packet boundaries are
preserved; partials are excluded; resumed speech within 500 ms stays one utterance;
concurrent finals remain in acoustic order; failures/timeouts do not retry or leak
provider details; hangup cancels requests without admitting unfinished speech.

TTS tests exercise separate LiveTTS text/audio messages, 8 kHz PCM and sample
alignment, first audio before stream completion, currency pronunciation, channel
reuse, barge-in, hangup, timeout, byte limits and failure after partial audio.
Configuration rejects mismatched model/voice families and batch speculation for
streaming STT. Default mode and existing provider selections are unchanged.

Streaming STT additionally accepts server `final_refinement` events: the
normalized text replaces the matching raw final segment by index, raw text is
kept when a refinement is missing or empty, and oversized or out-of-range
refinements fail explicitly without retry. Informational server messages
(session id, wall-clock-only and classifier/analysis updates) are ignored;
only final transcript text can enter Conversation. A live provider probe
(TTS-synthesized Russian phrase fed back through the exact live STT path)
returned a final with no errors after this change.

## Real provider gate

Least-privilege service account/key creation succeeded in the initially selected
Yandex Cloud account. The key was transferred over SSH into a root-only Voice
file; it is absent from this repository and test reports.

Synthetic API requests from the Voice host to both STT v3/general and TTS
v3/livetts-sofia + general-marina returned `PERMISSION_DENIED` for the folder/cloud.
The console independently reported billing-account suspension. A public free
usage notice does not establish that this particular account is operational.
No successful provider latency or voice-quality result is claimed. The user
requested switching to another account; credential/bootstrap work remains
external to this candidate's acceptance.

Before activation: successful synthetic STT/TTS at 8 kHz, measured first-audio and
final-STT timing, model/voice verification, exact native source/runtime pairing,
single-number routing readiness, and a controlled inbound call covering response,
barge-in and hangup. Do not switch the pilot on a permission-denied probe.

## Sources

- [Streaming STT v3](https://aistudio.yandex.ru/ru/docs/speechkit/stt/api/streaming-examples-v3)
- [Synthesis models and LiveTTS streaming options](https://aistudio.yandex.ru/ru/docs/speechkit/tts/models)
- [Model/voice compatibility](https://aistudio.yandex.ru/ru/docs/speechkit/tts/voices)
- [Logging control header](https://aistudio.yandex.ru/en/docs/speechkit/concepts/support-headers)
