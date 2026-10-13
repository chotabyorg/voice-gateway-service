# Telephony onboarding proposal for platform owners

Status: discussion input, updated 2026-10-13. Confirmed product constraint:
the service operates in Belarus and always supplies the phone numbers itself.
The UI and integration details below remain proposals, not an approved
number-routing protocol, deployment plan or permission to activate calls.

## First beta

Keep a small channel card labelled “Phone calls — beta, connection on request”.
Suggested description: “We provide a phone number in Belarus and connect it to
your AI agent. Connection is available on request.”

The initial request needs the desired platform agent and a contact for onboarding.
If multiple numbers are needed, optionally collect the quantity, defaulting to
one. Reuse business details already available in the platform. Agent selection
expresses the requested mapping and does not establish a working route.

Do not offer a country selector, bring-your-own-number, customer SIP/PBX or
forwarding setup. The customer does not choose a carrier or submit SIP
credentials or audio-provider API keys. Twilio is not part of this offering.

The action should create/request onboarding rather than claim instant
connection. Proposed visible progress: requested, waiting for details, technical
test, connected. “Connected” should require an accepted inbound test through the
intended route, not merely saved settings or successful SIP registration.
Product state ownership and the request destination need platform-owner review.

A separate tab becomes useful when customers manage multiple numbers, per-number
agents, routing, hours, human fallback destinations or call settings. For the
first beta, these can be collected during assisted onboarding instead of making
every customer complete a large technical form.

## Team-managed number provisioning

The team arranges a Belarusian number through its local telephony provider,
configures the provider/PBX route and completes an inbound acceptance test.
The customer sees the supplied number, selected agent and connection status.
Number availability, price and connection time should be confirmed during
onboarding; the card should not invent tariffs or promise immediate activation.

Carrier/PBX credentials, SIP registration or trunk authentication, networking
and media settings are managed internally by the team. They do not belong in
the customer form. Providing the number does not by itself implement the
trusted number-to-business/agent mapping described below.

[Asterisk AudioSocket](https://docs.asterisk.org/Latest_API/API_Documentation/Dialplan_Applications/AudioSocket/)
transports PCM and identifies a call with a UUID; it does not provision a phone
number or decide which platform agent owns the call.

## Current implementation evidence and open boundary

The inspected MTS `docs/telephony.md` at commit
`8fd51c236bf43060a788778139a3fbbc4d223be9` describes a separately configured
direct-SIP pilot with a server-selected workspace and internal extension.
Its historical live-call evidence is not evidence of integration with this new
Voice Gateway or of the current route of a public number. The older AudioSocket
path is documented there as an unverified alternative.

This service currently accepts an AudioSocket UUID plus audio. It does not have
trusted called-number/tenant/agent routing or production Conversation
integration. Copying MTS workspace selection, Room state or prompts would create
the wrong ownership boundary.

Platform/Conversation and PBX owners need to agree on:

1. Ownership and validation of the number-to-business/agent mapping, including
   ambiguous or unknown numbers and cross-business isolation.
2. How a trusted call identity obtains that context before the local
   `VoiceToConversationPort` is used; do not invent a public gateway protocol here.
3. Supported inbound routes, call acceptance criteria, human fallback and the
   behavior when the gateway or provider is unavailable.
4. Product handling of history, recording/consent and demo business actions.

An MTS demo can demonstrate the voice experience once its specific route and
test conditions are approved. Label it as a separate beta demonstration until
the new platform integration has been implemented and verified. This change
does not edit salon prompts, create an agent, switch a number route or deploy
either service.
