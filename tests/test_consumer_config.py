from unittest.mock import AsyncMock, Mock

import pytest

from app.main import Application
from app.workers.transcription import PROGRESS_INTERVAL_SECONDS


@pytest.mark.asyncio
async def test_subscribe_requests_explicit_consumer_config():
    """A newly created consumer must not be left on server defaults.

    Note this covers the creation path only: nats-py discards the config when the durable
    already exists, which is why documentation/jetstream-consumer-configuration.md carries
    one-off `nats consumer edit` steps.
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
    # The heartbeat must beat the ack timer, whichever value is in force.
    assert PROGRESS_INTERVAL_SECONDS < config.ack_wait
