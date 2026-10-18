# Russian booking pronunciation

SpeechKit's supported Russian voices receive a presentation-only expansion of
explicit E.164 phone numbers, valid numeric calendar dates, `в HH:MM` clock times
and the literal `Europe/Minsk` zone. Digits in a phone number are spoken one by
one; dates use month names; the zone becomes `по минскому времени`. Yearless
dates require an adjacent clock context, so decimal amounts stay unchanged.
Existing optional BYN expansion still preserves kopecks exactly.

Canonical authorized speech, Conversation history, business plans and receipts
retain their original values. The formatter is stateless per authorized chunk;
it does not join chunks or add playback delay. It cannot reconstruct a value
already split across separate authorized chunks. OpenRouter voices and provider
activation are unchanged.

Both provider character and UTF-8 limits apply after expansion, before opening
the synthesis RPC. Unit tests cover invalid dates, decimals, identifiers,
unmarked large numbers and unrelated zones; loopback gRPC tests verify the exact
provider request and expanded-text limits without external provider access.
