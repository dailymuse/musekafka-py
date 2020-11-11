"""Classes related to Message results from Kafka Consumers."""

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional

import muselog.context
from confluent_kafka import Consumer, Message
from muselog.logger import get_logger_with_context

import musekafka.shutdown
from musekafka.exceptions import ConsumerException

LOGGER = get_logger_with_context(logging.getLogger(__name__))

MILLIS_IN_SECOND = 1000

DEFAULT_POLL_TIMEOUT = 5.0
DEFAULT_BATCH_SIZE = 100
DEFAULT_BATCH_TIMEOUT = 30.0


def batch(
    consumer: Consumer,
    batch_count: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    batch_timeout: Optional[float] = DEFAULT_BATCH_TIMEOUT,
    poll_timeout: Optional[float] = DEFAULT_POLL_TIMEOUT,
    timeout: Optional[float] = None,
) -> Iterator[List[Message]]:
    """Stream batches of messages."""
    if batch_count is not None and batch_count <= 0:
        raise ValueError("batch_count must be a positive integer.")
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be a positive float.")

    if batch_count is None:
        LOGGER.debug("Streaming message batches....")
    else:
        LOGGER.debug("Streaming up to %d message batches....", batch_count)

    if timeout is not None:
        timeout_delta = timedelta(milliseconds=int(timeout * MILLIS_IN_SECOND))

    batch_duration = timedelta()
    num_batches = 0
    while batch_count is None or num_batches < batch_count:
        if musekafka.shutdown.is_shutting_down():
            break

        if timeout is not None:
            if batch_duration >= timeout_delta:
                LOGGER.debug("Hit batch timeout (%.3f seconds).", timeout)
                break
            batch_timeout = min(
                batch_timeout or timeout, (timeout_delta - batch_duration).total_seconds()
            )

        batch_start = datetime.utcnow()
        batch = list(
            stream(consumer, count=batch_size, poll_timeout=poll_timeout, timeout=batch_timeout)
        )
        batch_duration += datetime.utcnow() - batch_start
        if not batch:
            # Empty batch does not count towards num_batches,
            # since we require batch_size to be > 0.
            continue
        LOGGER.debug("Got batch of %d messages.", len(batch))
        num_batches += 1
        yield batch

    LOGGER.debug(
        "Completed streaming %d batches, with each batch of size at most %d.",
        num_batches,
        batch_size,
    )


def stream(
    consumer: Consumer,
    count: Optional[int] = None,
    poll_timeout: Optional[float] = DEFAULT_POLL_TIMEOUT,
    timeout: Optional[float] = None,
) -> Iterator[Message]:
    """Stream `count` messages from the consumer."""
    if count is not None and count <= 0:
        raise ValueError("count must be a positive integer.")
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be a positive float.")

    if count is None:
        LOGGER.debug("Streaming messages....")
    else:
        LOGGER.debug("Streaming up to %d messages....", count)

    if timeout is not None:
        timeout_delta = timedelta(milliseconds=int(timeout * MILLIS_IN_SECOND))

    stream_duration = timedelta()
    num_messages = 0
    while count is None or num_messages < count:
        if musekafka.shutdown.is_shutting_down():
            break

        if timeout is not None:
            if stream_duration >= timeout_delta:
                LOGGER.debug("Hit stream timeout (%.3f seconds).", timeout)
                break
            poll_timeout = min(
                poll_timeout or timeout, (timeout_delta - stream_duration).total_seconds()
            )

        stream_start = datetime.utcnow()
        try:
            message = poll(consumer, timeout=poll_timeout)
        except TimeoutError as e:
            LOGGER.debug(str(e))
            continue
        finally:
            stream_duration += datetime.utcnow() - stream_start
        ctx = dict(
            kafka_topic=message.topic(),
            kafka_partition=message.partition(),
            kafka_offset=message.offset(),
        )
        with _bind_ctx(ctx):
            yield message
        num_messages += 1
    LOGGER.debug("Completed streaming %d messages.", num_messages)


def poll(consumer: Consumer, timeout: Optional[float] = DEFAULT_POLL_TIMEOUT) -> Message:
    """Poll for a message from the consumer.

    This method differs from consumer.poll in that it will never return a null message
    or a message that has an error. If you set timeout and poll hits that timeout,
    it will raise a TimeoutError.
    """
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be a positive integer.")

    if timeout is not None:
        timeout_delta = timedelta(milliseconds=int(timeout * MILLIS_IN_SECOND))

    poll_duration = timedelta()
    while True:
        if timeout is not None:
            if poll_duration >= timeout_delta:
                raise TimeoutError(f"Hit poll timeout of {timeout} seconds.")
            poll_timeout = (timeout_delta - poll_duration).total_seconds()

        poll_start = datetime.utcnow()
        message = consumer.poll() if timeout is None else consumer.poll(timeout=poll_timeout)
        poll_duration += datetime.utcnow() - poll_start
        if message is None:
            continue

        topic = message.topic()
        partition = message.partition()
        offset = message.offset()

        LOGGER.debug(
            "Received message -- topic=%s partition=%s offset=%s",
            topic,
            partition,
            offset,
            kafka_topic=topic,
            kafka_partition=partition,
            kafka_offset=offset,
        )

        if message.error():
            # If we cannot read the raw message correctly, we give up and
            # raise the error to the caller.
            raise ConsumerException(f"Got bad message from Kafka: {message.error()}")

        return message


@contextmanager
def _bind_ctx(ctx: Dict[str, Any]) -> Iterator[None]:
    muselog.context.bind(**ctx)
    try:
        yield
    finally:
        # Clear only our context.
        # This will clear the `ctx` keys even if their values have been overwritten
        # by our yieldee.
        muselog.context.unbind(*list(ctx.keys()))
