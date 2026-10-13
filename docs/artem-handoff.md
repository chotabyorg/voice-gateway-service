# Artem handoff

This repository is the standalone Main Chota Voice Gateway foundation. Use
`AGENTS.md` as the durable ownership and escalation policy.

## Confirmed coordination gate — 2026-10-13

The platform team owns the Conversation side and is completing Goal5 Turn
Coordination. Wait for its acceptance and the team's frozen Voice/Conversation
contract plus exact Conversation implementation revision. Do not infer the
final Turn authority/fencing rules from the current seam or earlier channel code.

Until that handoff, continue local media hardening only. Keep
`VoiceToConversationPort` a local seam; do not connect legacy Speechik directly
to AI, route the number pool into the new path, activate a VOICE integration,
or change production routing/credentials.

The confirmed target is shared Voice Gateway ingress for the team's number pool.
The platform resolves a trusted DID binding to owner/integration/agent. A number
reservation (`RESERVED`) remains distinct from an active VOICE integration;
activation requires evidence for both routing readiness and the complete
Voice/Conversation path. See `docs/telephony-onboarding.md`.

After Goal5 acceptance, contract delivery and implementation, controlled acceptance
scope is only `+375173820015`: real inbound call, STT, Conversation, managed AI,
TTS/playback, interruption/barge-in and correct call termination. No other number
is authorized for activation by this decision. This records a future acceptance
scope, not permission to place a call or switch a route now.

## Artem / Voice team owns

- PBX/provider transports and AudioSocket.
- Audio codecs, frames, STT and TTS quality/provider adapters.
- VAD/end-of-speech, latency, jitter and media buffering.
- Playback, barge-in, acoustic behavior and call/media lifecycle.
- Provider-specific media edge cases, bounded operational buffering and voice
  observability.

## Artem / Voice team does not own

- LLM history, prompts, RAG product semantics or AI reasoning.
- Business capabilities, confirmation behavior, BusinessOperation, CRM or
  provider business mutation.
- Canonical Conversation Message history or AgentTurn scheduling authority.
- Human handoff product semantics, platform Inbox, campaigns or lead management.
- Recording retention policy or permanent recording storage.

`VoiceToConversationPort` and the dataclasses in `voice_gateway/events.py` are
local typed boundaries. They are not production-integrated and do not define an
external protocol. Preserve the invariant that generated speech is not proof of
heard speech. Report transport acceptance, partial delivery, interruption,
failure and unknown outcome distinctly.

## Stop conditions

If a task appears to require changing Conversation contracts, `ManagedTurnRequest`
or `ManagedTurnResult`, Message/AgentTurn ownership, shared delivery/playback
semantics, BusinessOperation, Prompt Runtime, Business capability policy,
CRM/provider mutation, HandoffCase/platform Inbox, recording retention/security
policy, or production cluster topology, stop and tell Artem:

> This crosses the Voice Gateway ownership boundary. Please consult the Main
> Conversation/Platform owners before proceeding.

Do not silently solve such decisions inside Voice. Do not change GitOps or
activate Goal4B/Telegram while working here.

## Suggested milestones

1. Harden the standalone media foundation: provider quality, real media edge
   cases, latency and bounded cancellation behavior, all against local fakes.
2. Design and review a local Voice-to-Conversation adapter with the Conversation
   owners; do not guess or widen its contract here.
3. Plan a controlled inbound real call only after explicit platform, PBX,
   security and operational approval.
4. Verify playback and barge-in truthfulness with the chosen transport/provider.
5. Design canonical call history, transcripts and artifacts later with the
   Conversation/platform owners, including retention and consent policy.

The default service stays inert. `VOICE_MODE=local_fake` is for deterministic
local integration only. Provider adapters are not activated by this app.

## Local media verification record

The standalone hardening changes and native acceptance results are recorded in
[`media-validation.md`](media-validation.md), including bounded queues, startup
failure cleanup, packet-independent STT/VAD, cancellation, a 960-call loopback
run and accelerated long-stream checks. Live provider quality and PBX acceptance
remain explicitly separate from these local results and the Goal5 contract gate.
