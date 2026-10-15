# Voice Gateway agent instructions

The user explicitly authorized implementing both sides of the Voice/Conversation
source bridge on 2026-10-15. Keep this candidate integration in separate PRs above
Goal5; production activation, number routing and GitOps remain separately gated.
This authorizes the candidate HTTP adapter without making the local port itself
a frozen production protocol. Do native verification without local Docker.

Read this file and `docs/architecture.md` before changing service ownership,
provider activation, playback facts, or the Conversation boundary.

## Voice owns

- PBX/provider transports and AudioSocket.
- Audio codecs and frame handling.
- STT/TTS adapters and quality work.
- VAD, end-of-speech, acoustic utterance coordination, latency and jitter.
- Playback, barge-in, call/media lifecycle and provider-specific media cases.
- Bounded operational buffering and voice observability.

## Voice does not own

- LLM history, prompts, RAG product semantics or AI reasoning.
- Business capabilities, confirmation policy, BusinessOperation or CRM behavior.
- Canonical Conversation Messages or AgentTurn scheduling authority.
- HandoffCase/platform Inbox, campaign, appointment or lead semantics.
- Recording retention policy or permanent recording/artifact storage.

Generated speech is not evidence that a caller heard it. Only report transport
acceptance and playback lifecycle facts. Keep interruption, partial delivery,
failure and unknown outcome distinct from completion.

The default service mode is inert. Do not make a real transport/provider active
by default, and do not add production credentials to local configuration.
`local_fake` is deterministic test plumbing, not a production provider mode.

## Stop and escalate

If work appears to require changing any of these, stop and tell Artem:

> This crosses the Voice Gateway ownership boundary. Please consult the Main
> Conversation/Platform owners before proceeding.

Escalation areas:

- Conversation external/internal contracts or `ManagedTurnRequest` /
  `ManagedTurnResult`.
- Message/AgentTurn ownership or shared delivery/playback semantics.
- BusinessOperation, Prompt Runtime, Business capability policy, CRM/provider
  business mutation, or confirmation behavior.
- HandoffCase/platform Inbox ownership.
- Recording retention/security product policy.
- Production cluster topology, GitOps or production credentials.

Do not solve cross-track decisions locally or activate Goal4B / Telegram as a
side effect of Voice work.

## Local workflow

- Python 3.11; install `requirements-dev.txt`, then run `ruff check .`,
  `ruff format --check .`, `mypy`, and `pytest -q`.
- Container verification uses the disposable local-fake stack. It must not
  contact external providers or public telephony.
- No SQLite, local canonical history, recording archive or spool is present.
- Keep logs structured and exclude transcript text, phone numbers and secrets.
- Follow normal feature-branch and pull-request flow in this repository.
