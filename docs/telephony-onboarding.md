# Telephony onboarding proposal for platform owners

Status: discussion input, 2026-10-13. This is not an approved product contract,
number-routing protocol, deployment plan or permission to activate calls.

## First beta

Keep a small channel card labelled “Phone calls — beta, connection on request”.
The initial request needs country, existing number or need for a new number,
carrier/PBX if known, desired platform agent and a technical contact. Existing
numbers can be a small list; an agent selection expresses the requested mapping
and does not establish a working route. Do not ask for provider audio API keys
in this customer form.

The action should create/request onboarding rather than claim instant
connection. Proposed visible progress: requested, waiting for details, technical
test, connected. “Connected” should require an accepted inbound test through the
intended route, not merely saved settings or successful SIP registration.
Product state ownership and the request destination need platform-owner review.

A separate tab becomes useful when customers manage multiple numbers, per-number
agents, routing, hours, human fallback destinations or call settings. For the
first beta, these can be collected during assisted onboarding instead of making
every customer complete a large technical form.

## Why there is no universal phone token

| Connection path | Customer supplies | Technical work |
|---|---|---|
| Existing number, forwarding available | Existing number, carrier and forwarding capabilities | Arrange a reachable destination, forwarding and an inbound test; verify which called/calling-number metadata survives. |
| Existing SIP account, trunk or PBX | Carrier/PBX, numbers and technical contact | Confirm authentication mode and route, exchange the required SIP parameters securely, verify media reachability and test calls. |
| New number | Country/region, number requirements and intended agent | Choose and provision a supported carrier/number, then configure and test the route. Availability and onboarding requirements depend on that carrier. |

For SIP, parameters may include registrar/server and port, domain, transport,
username/auth ID and password **or** IP-based authentication. Outbound proxy,
VPN/network restrictions and RTP/codec settings depend on the PBX. These belong
in assisted technical setup, not one unconditional customer credential form.
SIP credentials and OpenRouter keys serve different systems.

Examples from primary documentation: [Twilio SIP trunk configuration](https://www.twilio.com/docs/sip-trunking)
separates phone-number attachment, SIP routing and authentication.
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
