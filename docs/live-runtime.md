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
- `STT_PROVIDER` (`groq` or `openrouter`), `STT_API_KEY`, `STT_MODEL`
- `TTS_PROVIDER` (`edge` or `openrouter`), `TTS_VOICE`
- For OpenRouter TTS: `TTS_API_KEY`, `TTS_MODEL`

Optional `STT_LANGUAGE` defaults to `ru`. Credentials belong in a protected
service environment, never source or browser settings. Selected provider
dependencies and ffmpeg are required before the listener starts. Each call owns
its speech clients; runtime shutdown drains calls then closes boundary clients.

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
