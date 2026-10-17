"""Composition root for the inert service and deterministic local-fake mode."""

import asyncio
import logging
import math
import struct
from dataclasses import dataclass

from voice_gateway.events import AuthorizedSpeech
from voice_gateway.media.audiosocket import AudioSocketTransport
from voice_gateway.media.batch_stt import BatchSTT, BatchSTTSettings
from voice_gateway.media.call import MediaCall, MediaSettings
from voice_gateway.media.completion import bounded_call, finish_owned_task, observe_task
from voice_gateway.media.interfaces import BaseTTS
from voice_gateway.media.vad import VADSettings
from voice_gateway.ports import VoiceToConversationPort
from voice_gateway.settings import Settings

logger = logging.getLogger(__name__)


class LocalFakeSTT(BatchSTT):
    """Deterministic fake used only after explicit VOICE_MODE=local_fake."""

    async def _transcribe(self, pcm):
        return "local test utterance"


class LocalFakeTTS(BaseTTS):
    """Small deterministic 440 Hz signal; never contacts a speech provider."""

    async def synthesize(self, text):
        if len(text) > 4000:
            raise ValueError("Speech text exceeds the local safety limit")
        samples = [int(1200 * math.sin(2 * math.pi * 440 * n / 8000)) for n in range(800)]
        return struct.pack("<" + "h" * len(samples), *samples)


class LocalFakeConversationPort:
    """Example local adapter that exercises the typed seam without network I/O."""

    async def call_started(self, event):
        return None

    async def final_utterance(self, event):
        return AuthorizedSpeech(request_id=f"local-{event.sequence}", text="Local test response")

    async def playback_fact(self, event):
        return None

    async def call_ended(self, event):
        return None


@dataclass(frozen=True)
class RuntimeStatus:
    ready: bool
    mode: str
    audiosocket_port: int | None
    active_calls: int


class VoiceGatewayRuntime:
    def __init__(
        self,
        settings: Settings,
        *,
        conversation_port: VoiceToConversationPort | None = None,
        stt_factory=None,
        tts_factory=None,
        speech_classifier_factory=None,
        turn_silence_seconds=0.5,
        idle_prompt_seconds=0.0,
        max_idle_prompts=2,
    ):
        self.settings = settings
        self.turn_silence_seconds = turn_silence_seconds
        self.idle_prompt_seconds = idle_prompt_seconds
        self.max_idle_prompts = max_idle_prompts
        if settings.mode in {"live_audiosocket", "live_sip"} and any(
            value is None for value in (conversation_port, stt_factory, tts_factory)
        ):
            raise ValueError("Live mode requires explicit Conversation and speech providers")
        self.conversation_port = conversation_port or LocalFakeConversationPort()
        self.stt_factory = stt_factory or (
            lambda: LocalFakeSTT(
                vad=VADSettings(max_utterance_ms=30000), queue=BatchSTTSettings(max_pending=4)
            )
        )
        self.tts_factory = tts_factory or LocalFakeTTS
        self.speech_classifier_factory = speech_classifier_factory
        self.server = None
        self._sessions = {}
        self._closing = False

    @property
    def ready(self):
        return self.settings.mode != "disabled" and self.server is not None and not self._closing

    @property
    def bound_port(self):
        if self.server is None or not self.server.sockets:
            return None
        return int(self.server.sockets[0].getsockname()[1])

    def status(self):
        return RuntimeStatus(self.ready, self.settings.mode, self.bound_port, len(self._sessions))

    async def start(self):
        self._closing = False
        if self.settings.mode == "disabled":
            logger.info(
                "voice_runtime_inert", extra={"event": "voice_runtime_inert", "mode": "disabled"}
            )
            return
        self.server = await asyncio.start_server(
            self._handle_connection,
            host=self.settings.audiosocket_host,
            port=self.settings.audiosocket_port,
            limit=self.settings.max_frame_bytes + 3,
            backlog=self.settings.max_calls,
        )
        logger.info(
            "voice_runtime_ready",
            extra={
                "event": "voice_runtime_ready",
                "mode": self.settings.mode,
                "audiosocket_port": self.bound_port,
            },
        )

    async def stop(self):
        self._closing = True
        logger.info(
            "voice_shutdown_started",
            extra={"event": "voice_shutdown_started", "active_calls": len(self._sessions)},
        )
        server, self.server = self.server, None
        if server is not None:
            server.close()
            logger.info("voice_listener_closed", extra={"event": "voice_listener_closed"})
        sessions = tuple(self._sessions.items())
        tasks = [task for task, _ in sessions if task is not asyncio.current_task()]
        if tasks:
            for task in tasks:
                task.cancel("voice service shutdown")
            logger.info(
                "voice_sessions_cancelled",
                extra={"event": "voice_sessions_cancelled", "active_calls": len(tasks)},
            )
            done, pending = await asyncio.wait(
                tasks, timeout=self.settings.shutdown_timeout_seconds
            )
            logger.info(
                "voice_sessions_drained",
                extra={
                    "event": "voice_sessions_drained",
                    "active_calls": len(tasks) - len(pending),
                },
            )
            # Force-close concurrently; the grace period must not grow per call.
            await asyncio.gather(
                *(
                    self._close_unowned((("transport", self._sessions[task].close),))
                    for task in pending
                    if task in self._sessions
                ),
                return_exceptions=True,
            )
            for task in pending:
                task.cancel()
            if pending:
                drained, still_pending = await asyncio.wait(pending, timeout=1)
                done.update(drained)
                if still_pending:
                    for task in still_pending:
                        task.add_done_callback(observe_task)
                    logger.error(
                        "voice_shutdown_tasks_pending",
                        extra={
                            "event": "voice_shutdown_tasks_pending",
                            "active_calls": len(still_pending),
                        },
                    )
            await asyncio.gather(*done, return_exceptions=True)
        if server is not None:
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=1)
                logger.info("voice_listener_drained", extra={"event": "voice_listener_drained"})
            except TimeoutError:
                logger.error(
                    "voice_listener_drain_timeout", extra={"event": "voice_listener_drain_timeout"}
                )
        logger.info("voice_shutdown_complete", extra={"event": "voice_shutdown_complete"})

    async def _handle_connection(self, reader, writer):
        transport = AudioSocketTransport(
            reader,
            writer,
            max_frame_bytes=self.settings.max_frame_bytes,
            idle_timeout=self.settings.audiosocket_idle_timeout_seconds,
        )
        await self.handle_transport(transport)

    async def handle_transport(self, transport):
        """Own one accepted transport and all per-call resources."""
        task = asyncio.current_task()
        if self._closing or len(self._sessions) >= self.settings.max_calls:
            await finish_owned_task(
                asyncio.create_task(self._close_unowned((("transport", transport.close),)))
            )
            return
        if task is not None:
            self._sessions[task] = transport
        unowned = [("transport", transport.close)]
        try:
            stt = self.stt_factory()
            unowned.append(("stt", stt.stop_stream))
            tts = self.tts_factory()
            unowned.append(("tts", tts.close))
            call = MediaCall(
                transport,
                stt,
                tts,
                self.conversation_port,
                settings=MediaSettings(
                    max_call_seconds=self.settings.max_call_seconds,
                    turn_silence_seconds=self.turn_silence_seconds,
                    idle_prompt_seconds=self.idle_prompt_seconds,
                    max_idle_prompts=self.max_idle_prompts,
                ),
                interruption_classifier=(
                    self.speech_classifier_factory() if self.speech_classifier_factory else None
                ),
            )
            unowned.clear()  # run() owns cleanup from its first suspension onward.
            await call.run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "voice_call_failed",
                extra={
                    "event": "voice_call_failed",
                    "error_type": type(exc).__name__,
                },
            )
        finally:
            try:
                if unowned:
                    await finish_owned_task(asyncio.create_task(self._close_unowned(unowned)))
            finally:
                if task is not None:
                    self._sessions.pop(task, None)

    async def _close_unowned(self, resources):
        """Close a rejected/partially built call without skipping later resources."""
        cancellation = None
        for name, close in resources:
            try:
                completed = await bounded_call(
                    close(),
                    timeout=min(1, self.settings.shutdown_timeout_seconds),
                    cancellation_grace=0,
                )
                if not completed:
                    logger.warning(
                        "media_cleanup_timeout",
                        extra={
                            "event": "media_cleanup_timeout",
                            "resource": name,
                        },
                    )
            except asyncio.CancelledError as exc:
                cancellation = cancellation or exc
            except Exception as exc:
                logger.warning(
                    "media_cleanup_error",
                    extra={
                        "event": "media_cleanup_error",
                        "resource": name,
                        "error_type": type(exc).__name__,
                    },
                )
        if cancellation is not None:
            raise cancellation
