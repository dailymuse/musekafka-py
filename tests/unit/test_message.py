"""Test Kafka message polling, streaming, and batching."""

from typing import List
from unittest.mock import Mock

import pytest

from musekafka import exceptions, message

from . import FakeMessage, make_side_effect


@pytest.fixture
def consumer_mock() -> Mock:
    return Mock()


@pytest.fixture
def messages() -> List[FakeMessage]:
    return [FakeMessage("test", offset=y) for y in range(0, 20)]


def test_poll_skips_null_messages(consumer_mock: Mock):
    """message.poll does not return null messages."""
    msg1 = FakeMessage("test")
    msg2 = FakeMessage("test", offset=1)
    consumer_mock.poll.side_effect = [None, None, msg1, None, msg2]
    assert message.poll(consumer_mock) == msg1
    assert message.poll(consumer_mock) == msg2


def test_poll_raises_error_for_message_with_error(consumer_mock: Mock):
    """message.poll raises an exception for message that has a non-null error value."""
    msg1 = FakeMessage("test", error="ah, crap.")
    consumer_mock.poll.return_value = msg1
    with pytest.raises(exceptions.ConsumerException):
        message.poll(consumer_mock)


def test_poll_raises_timeout_error_on_timeout(consumer_mock: Mock):
    """message.poll raises a timeout error."""
    consumer_mock.poll.side_effect = make_side_effect(
        [None, None, None, FakeMessage("test")], delay=0.1
    )
    with pytest.raises(TimeoutError):
        message.poll(consumer_mock, timeout=0.01)


def test_stream_count_exact(consumer_mock: Mock):
    """message.stream ignores null messages and respects count."""
    msg1 = FakeMessage("test")
    msg2 = FakeMessage("test", offset=1)
    msg3 = FakeMessage("test", offset=2)
    consumer_mock.poll.side_effect = [msg1, None, msg2, msg3]
    assert list(message.stream(consumer_mock, count=2)) == [msg1, msg2]


def test_stream_count_timeout(consumer_mock: Mock, messages: List[FakeMessage]):
    """message.stream respects count and timeout."""
    # Should be able to consume all messages up to count if the timeout is long enough.
    consumer_mock.poll.side_effect = make_side_effect(messages, delay=0.01)
    assert list(message.stream(consumer_mock, count=len(messages), timeout=10)) == messages

    # If we set the timeout quite low, we should not be able to consume all messages
    consumer_mock.poll.side_effect = make_side_effect(messages, delay=0.01)
    assert len(list(message.stream(consumer_mock, count=len(messages), timeout=0.07))) != len(
        messages
    )


def test_stream_timeout(consumer_mock: Mock, messages: List[FakeMessage]):
    """message.stream times out if no count is specified but a timeout is specified."""
    # Will provide as many messages as possible within a timeout period
    consumer_mock.poll.side_effect = make_side_effect(messages, delay=0.01)
    assert len(list(message.stream(consumer_mock, count=None, timeout=0.05))) <= len(messages)


def test_batch_count_exact(consumer_mock: Mock, messages: List[FakeMessage]) -> None:
    """message.batch supplies exactly the number of specified batches of a specified size."""
    # Test 1 batch of 10 messages
    consumer_mock.poll.side_effect = make_side_effect(messages)
    assert list(message.batch(consumer_mock, batch_count=1, batch_size=10)) == [messages[:10]]

    # Test 1 batch of 20 messages
    consumer_mock.poll.side_effect = make_side_effect(messages)
    assert list(message.batch(consumer_mock, batch_count=1, batch_size=len(messages))) == [
        messages
    ]

    # Test 2 batches of 5 messages
    consumer_mock.poll.side_effect = make_side_effect(messages)
    assert list(message.batch(consumer_mock, batch_count=2, batch_size=5)) == [
        messages[:5],
        messages[5:10],
    ]

    # Test 2 batches of 10 messages
    consumer_mock.poll.side_effect = make_side_effect(messages)
    assert list(message.batch(consumer_mock, batch_count=2, batch_size=10)) == [
        messages[:10],
        messages[10:],
    ]

    # Test 3 batches of 6 messages
    consumer_mock.poll.side_effect = make_side_effect(messages)
    assert list(message.batch(consumer_mock, batch_count=3, batch_size=6)) == [
        messages[:6],
        messages[6:12],
        messages[12:18],
    ]

    # Test 20 batches of 1 message
    consumer_mock.poll.side_effect = make_side_effect(messages)
    assert list(message.batch(consumer_mock, batch_count=len(messages), batch_size=1)) == [
        [m] for m in messages
    ]


def test_batch_count_timeout(consumer_mock: Mock, messages: List[FakeMessage]) -> None:
    """message.batch supplies count or less messages within a specified timeout."""
    # Should be able to consume all messages up to count if the timeout is long enough.
    consumer_mock.poll.side_effect = make_side_effect(messages, delay=0.01)
    assert list(message.batch(consumer_mock, batch_count=2, batch_size=10, batch_timeout=10)) == [
        messages[:10],
        messages[10:],
    ]

    # If we set the timeout quite low, we should not be able to consume the entirety of the batches
    consumer_mock.poll.side_effect = make_side_effect(messages, delay=0.01)
    batch1, batch2 = list(
        message.batch(consumer_mock, batch_count=2, batch_size=10, batch_timeout=0.07)
    )
    assert len(batch1) < 10
    assert len(batch2) < 10


def test_batch_timeout(consumer_mock: Mock, messages: List[FakeMessage]):
    """message.batch times out if no count is specified but a timeout is specified."""
    # Will provide as many messages as possible within a timeout period
    consumer_mock.poll.side_effect = make_side_effect(messages, delay=0.01)
    assert len(list(message.batch(consumer_mock, batch_count=None, timeout=0.05))) <= len(messages)
