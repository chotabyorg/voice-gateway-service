# Supplied-number onboarding and Voice integration boundary

Status: target direction confirmed by the platform team on 2026-10-13.
This supersedes the earlier assisted-onboarding proposal. It does not define a
production wire protocol or authorize routing changes before the Goal5 handoff.

## Customer flow and platform state

Chota supplies Belarusian numbers from its own Business Network (`bn.by`) pool.
The card belongs to the current agent's channels page. Customers provide a
connection contact and number quantity; they do not configure SIP credentials,
pick a second agent on that page, or bring their own carrier in this beta.

Pool reservation is implemented in Control Plane and exposed by the platform.
`RESERVED` establishes allocation, not working telephony. A working `VOICE`
integration is a separate platform state. The platform may activate it only
after routing readiness and the complete Voice/Conversation path are proven.
Voice must not promote a reservation or infer activation from SIP registration.

## Confirmed target path

```text
PBX / DID -> Voice Gateway -> Conversation -> managed AI
managed AI -> Conversation -> Voice Gateway -> TTS/playback -> PBX / caller
```

Numbers in the pool should reach shared Voice Gateway ingress. The platform
resolves a trusted called-number binding to owner/integration/agent. The goal is
automatic assignment and readiness checks, without separate PBX setup for each
customer. The mechanism for conveying trusted DID/call context is part of the
pending platform contract; this document does not invent it.

AudioSocket currently carries a UUID plus audio in this repository. That UUID
alone does not prove number ownership or identify an authorized platform agent.
Existing MTS/direct-SIP pilot behavior and the old Speechik-to-AI adapter are
donor references, not production integration contracts for this gateway.
Do not reconnect legacy Speechik directly to AI.

## Ownership and the Goal5 gate

The platform team implements the Conversation side. It will deliver a frozen
Voice/Conversation contract and an exact implementation revision after Goal5
Turn Coordination acceptance, including the final authority/fencing semantics.

The Voice team owns PBX/AudioSocket, codecs, STT/TTS, VAD, interruption/barge-in,
playback and the call/media lifecycle. Until that handoff:

- Continue local media hardening with fake providers and loopback transports.
- Keep `VoiceToConversationPort` and its events a local seam.
- Do not invent Conversation endpoints or alter ManagedTurn/shared playback
  contracts, canonical history, AI or Business/CRM ownership.
- Do not route the number pool into the new path, activate integrations, add
  production credentials or change the service's inert default.

## Controlled acceptance after implementation

The agreed future acceptance scope is one number: `+375173820015`.
After Goal5 acceptance, frozen contract delivery and implementation, verify:

1. An actual inbound call obtains the correct trusted owner/integration/agent.
2. Caller audio goes through STT, Conversation and managed AI; authorized output
   returns through Conversation to Voice for TTS and playback.
3. Interruption/barge-in stops local output and reports the contract's playback
   facts truthfully; generated audio is not proof of caller hearing.
4. Hangup, failure and cancellation release the call's media resources.

Keep all other numbers inactive. No live call, activation or routing switch is
authorized before the Goal5 handoff by the confirmation of this target scheme.
