"""Local application ports, independent of the candidate HTTP adapter's wire DTOs."""

from collections.abc import AsyncIterator
from typing import NoReturn, Protocol

from voice_gateway.events import (
    AuthorizedSpeech,
    CallEnded,
    CallStarted,
    FinalUtterance,
    PlaybackFact,
)


class VoiceToConversationPort(Protocol):
    """Typed local seam; production integration requires separate acceptance."""

    async def call_started(self, event: CallStarted) -> None: ...

    async def final_utterance(self, event: FinalUtterance) -> AuthorizedSpeech | None: ...

    async def playback_fact(self, event: PlaybackFact) -> None: ...

    async def call_ended(self, event: CallEnded) -> None: ...


class StreamingConversationPort(VoiceToConversationPort, Protocol):
    """Optional asynchronous output so admission never waits for AI or playback."""

    def responses(self, call_id: str) -> AsyncIterator[AuthorizedSpeech]: ...

    async def wait_failure(self, call_id: str) -> NoReturn: ...
