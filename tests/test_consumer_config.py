from unittest.mock import AsyncMock, Mock

import pytest

from app.main import Application
from app.workers.transcription import PROGRESS_INTERVAL_SECONDS


@pytest.mark.asyncio
async def test_subscribe_requests_explicit_consumer_config():
    """A newly created consumer must not be left on server defaults.

    Note this covers the creation path only: nats-py discards the config when the durable
    already exists, which is why the README carries a one-off `nats consumer edit`.
    """
    app = Application()
    app.js = AsyncMock()
    app.worker = Mock()
    app.worker.handle_message = AsyncMock()

    await app._subscribe_consumers()

    app.js.subscribe.assert_awaited_once()
    config = app.js.subscribe.await_args.kwargs["config"]

    assert config.ack_wait == 300.0
    assert config.max_deliver == 3
    assert config.max_ack_pending == 1


def test_heartbeat_outpaces_the_server_default_ack_wait():
    """Until the one-off `nats consumer edit` runs, the consumer still holds ack_wait=30s.

    An interval sized against our own 300s would send its first +WPI after the message had
    already been redelivered.
    """
    assert PROGRESS_INTERVAL_SECONDS < 30
