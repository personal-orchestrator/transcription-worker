import asyncio
import logging
import signal
import os
from dataclasses import dataclass
from typing import Callable, Awaitable, Any, List

import nats
from nats.aio.client import Client as NatsClient
from nats.js import JetStreamContext, api

from app.config import settings
from app.services.transcription import GroqTranscriptionService, TranscriptionService
from app.workers.transcription import TranscriptionWorker
from app.reindexer import Reindexer

@dataclass(frozen=True)
class StreamConfig:
    name: str
    subjects: List[str]

@dataclass(frozen=True)
class SubscriptionConfig:
    subject: str
    cb: Callable[[Any], Awaitable[None]]
    durable: str
    stream: str

# Server defaults (ack_wait=30s, max_deliver=-1, max_ack_pending=1000) are wrong for a single
# worker doing slow, rate-limited work. ACK_WAIT_SECONDS is a death-detection window, not a
# duration budget — the msg.in_progress() heartbeat covers duration.
# See documentation/jetstream-consumer-configuration.md.
ACK_WAIT_SECONDS = 300.0
MAX_DELIVER = 3
MAX_ACK_PENDING = 1

logging.basicConfig(level=settings.log_level, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("transcription-worker")


class Application:
    """Main application lifecycle manager for the transcription worker service."""

    def __init__(self):
        self.nc: NatsClient = nats.NATS()
        self.js: JetStreamContext = None  # type: ignore[assignment]
        self.stop_event = asyncio.Event()
        self.subscriptions: list = []
        self.worker: TranscriptionWorker = None  # type: ignore[assignment]

    async def run(self) -> None:
        """Execute application setup, execution, and graceful cleanup."""
        try:
            await self._connect_nats()
            self._init_worker()
            await self._ensure_streams()
            await self._subscribe_consumers()
            self._setup_signal_handlers()

            watcher_task = asyncio.create_task(self._watch_for_reindex())

            await self.stop_event.wait()
            watcher_task.cancel()
        finally:
            await self._cleanup()

    async def _connect_nats(self) -> None:
        logger.info(f"Connecting to NATS at {settings.nats_url}")
        await self.nc.connect(settings.nats_url, connect_timeout=10)
        self.js = self.nc.jetstream()
        logger.info("Connected to NATS JetStream")

    def _init_worker(self) -> None:
        transcription_service = self._create_transcription_service()
        self.worker = TranscriptionWorker(
            transcription_service=transcription_service,
            storage_dir=settings.storage_dir,
            transcriptions_raw_dir=settings.transcriptions_raw_dir,
            transcriptions_dir=settings.transcriptions_dir,
            nc=self.nc,
            nats_transcriptions_subject=settings.nats_transcriptions_subject,
        )

    def _create_transcription_service(self) -> TranscriptionService:
        """Factory method constructing the transcription engine implementation."""
        return GroqTranscriptionService(
            api_key=settings.groq_api_key,
            rate_limit_per_minute=settings.groq_rate_limit_per_minute,
        )

    async def _ensure_streams(self) -> None:
        streams = [
            StreamConfig(name="audio_events", subjects=[settings.nats_subject]),
            StreamConfig(name="processing_events", subjects=[settings.nats_transcriptions_subject]),
        ]
        for stream in streams:
            try:
                await self.js.add_stream(name=stream.name, subjects=stream.subjects)
                logger.info(f"JetStream stream '{stream.name}' ensured")
            except Exception as e:
                logger.info(f"Stream '{stream.name}' check/creation note: {e}")

    async def _subscribe_consumers(self) -> None:
        configs = [
            SubscriptionConfig(
                subject=settings.nats_subject,
                cb=self.worker.handle_message,
                durable="transcription-worker-consumer",
                stream="audio_events",
            ),
        ]
        for cfg in configs:
            config = api.ConsumerConfig(
                ack_wait=ACK_WAIT_SECONDS,
                max_deliver=MAX_DELIVER,
                max_ack_pending=MAX_ACK_PENDING,
            )
            sub = await self.js.subscribe(
                cfg.subject,
                cb=cfg.cb,
                durable=cfg.durable,
                stream=cfg.stream,
                config=config,
            )
            self.subscriptions.append(sub)
            logger.info(f"Subscribed to {cfg.subject} (durable: {cfg.durable}, stream: {cfg.stream})")

    def _setup_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._signal_handler)

    def _signal_handler(self) -> None:
        logger.info("Shutdown signal received")
        self.stop_event.set()

    async def _watch_for_reindex(self) -> None:
        reindex_file = os.path.join(os.path.dirname(settings.storage_dir), "reindex")
        logger.info(f"Starting reindex watcher on {reindex_file}, polling every {settings.reindex_poll_interval}s")

        reindexer = Reindexer(
            storage_dir=settings.storage_dir,
            transcriptions_dir=settings.transcriptions_dir,
            nc=self.nc,
            nats_subject=settings.nats_subject,
        )

        while not self.stop_event.is_set():
            try:
                if os.path.exists(reindex_file):
                    await self._trigger_reindex(reindex_file, reindexer)
            except Exception as e:
                logger.error(f"Error in reindex watcher loop: {e}")

            try:
                await asyncio.sleep(settings.reindex_poll_interval)
            except asyncio.CancelledError:
                break

    async def _trigger_reindex(self, reindex_file: str, reindexer: Reindexer) -> None:
        try:
            os.remove(reindex_file)
            logger.info("Reindex trigger file detected and removed. Triggering reindex.")
            await reindexer.run()
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error(f"Error executing reindex: {e}")

    async def _cleanup(self) -> None:
        logger.info("Unsubscribing and closing NATS connection")
        for sub in self.subscriptions:
            try:
                await sub.unsubscribe()
            except Exception as e:
                logger.warning(f"Error unsubscribing: {e}")

        if self.nc.is_connected:
            await self.nc.close()
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    app = Application()
    asyncio.run(app.run())
