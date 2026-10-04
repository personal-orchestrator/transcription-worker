from unittest.mock import AsyncMock, Mock

import pytest

from app.main import Application
from app.workers.transcription import PROGRESS_INTERVAL_SECONDS


@pytest.mark.asyncio
async def test_subscribe_requests_explicit_consumer_config():
    """A newly created consumer must not be left on server defaults."""
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
    # The heartbeat must beat the ack timer.
    assert PROGRESS_INTERVAL_SECONDS < config.ack_wait
