# Future deployment contract (documentation only)

This file records future constraints. It does not add production manifests,
public ingress, credentials or deployment automation.

- The process is inert when `VOICE_MODE=disabled` (the default). Only an
  explicit local `local_fake` setting starts the included AudioSocket listener.
- This image is not production-ready: no Conversation adapter, provider
  composition, provider authentication, PBX admission policy or production
  call controls are implemented.
- Future provider activation must remain private to the approved telephony
  network. Do not expose AudioSocket to public ingress or publish its port
  broadly. The local fake Compose example binds both ports to host loopback and
  uses a dedicated bridge so the host can run the fake client; it configures no
  outbound provider clients. CI runs the same container checks on an internal
  no-egress network.
- Future Conversation integration must use the reviewed platform contract and
  the typed seam. Add authentication and authorization at that integration
  boundary; do not trust caller-supplied identity or provider metadata.
- No audio, transcript, phone number or credential should be written to logs.
  This service has no durable data directory or writable application path.
- Runtime containers should remain non-root, read-only, with only a bounded
  temporary filesystem if a later reviewed provider requires one. Provider
  subprocesses need explicit input/output limits, timeouts and reaping.
- Readiness is false while transport mode is disabled. Liveness only confirms
  the HTTP process is responding; neither endpoint is an administrative API.
- Production calls, recording retention, transcripts and artifacts require
  separate product/security review and explicit consent/retention decisions.

No GitOps, Kubernetes, Argo, PBX, SIP trunk, DNS or public ingress configuration
is included here.
