"""Application ports. The Conversation seam has no network implementation here."""

from typing import Protocol

from voice_gateway.events import (
    AuthorizedSpeech,
    CallEnded,
    CallStarted,
    FinalUtterance,
    PlaybackFact,
)


class VoiceToConversationPort(Protocol):
    """Typed local seam to adapt in a later, separately reviewed integration."""

    async def call_started(self, event: CallStarted) -> None: ...

    async def final_utterance(self, event: FinalUtterance) -> AuthorizedSpeech | None: ...

    async def playback_fact(self, event: PlaybackFact) -> None: ...

    async def call_ended(self, event: CallEnded) -> None: ...
