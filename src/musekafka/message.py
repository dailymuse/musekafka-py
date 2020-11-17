"""Classes related to Message results from Kafka Consumers."""

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Iterator, List, Optional

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
    """Stream batches of messages.

    Args:
        consumer (Consumer): Consumer that provides the messages.
        batch_count (Optional[int], optional): Number of batches to stream.
            If `None`, stream forever or until timeout.
            Defaults to None.
        batch_size (int, optional): Number of messages in a batch.
            Defaults to DEFAULT_BATCH_SIZE.
        batch_timeout (Optional[float], optional): Seconds to accumulate a batch of messages.
            Defaults to DEFAULT_BATCH_TIMEOUT.
        poll_timeout (Optional[float], optional): Seconds to block on polling of a single message.
            If `None`, block indefinitely.
            Defaults to DEFAULT_POLL_TIMEOUT.
        timeout (Optional[float], optional): Cumulative seconds to block fetching messages before
            exiting.
            If `None`, never exit (unless batch_count is specified).
            Defaults to None.

    Raises:
        ValueError: If batch_count is not positive.
        ValueError: If batch_size is not positive.
        ValueError: If batch_timeout is not positive.
        ValueError: If poll_timeout is not positive.
        ValueError: If timeout is not positive.

    Yields:
        List[Message]: A batch of messages.
    """
    if batch_count is not None and batch_count <= 0:
        raise ValueError("batch_count must be a positive integer.")
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")
    if batch_timeout is not None and batch_timeout <= 0:
        raise ValueError("batch_timeout must be a positive float.")
    if poll_timeout is not None and poll_timeout <= 0:
        raise ValueError("poll_timeout must be a positive float.")
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
    """Stream `count` messages from the consumer.

    Args:
        consumer (Consumer): Consumer that provides the messages.
        count (Optional[int], optional): Number of messages to consume before exiting.
            If `None`, stream will continue polling messages forever or until timeout.
            Defaults to None.
        poll_timeout (Optional[float], optional): Seconds to block on polling of a single message.
            If `None`, block indefinitely.
            Defaults to DEFAULT_POLL_TIMEOUT.
        timeout (Optional[float], optional): Cumulative seconds to block fetching messages before
            exiting.
            If `None`, never exit (unless count is specified).
            Defaults to None.

    Raises:
        ValueError: If count is not positive.
        ValueError: If poll_timeout is not positive.
        ValueError: If timeout is not positive.

    Yields:
        Message: A Kafka message from the message stream.
    """
    if count is not None and count <= 0:
        raise ValueError("count must be a positive integer.")
    if poll_timeout is not None and poll_timeout <= 0:
        raise ValueError("poll_timeout must be a positive float.")
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
        with bind_message_ctx(message):
            yield message
        num_messages += 1
    LOGGER.debug("Completed streaming %d messages.", num_messages)


def poll(consumer: Consumer, timeout: Optional[float] = DEFAULT_POLL_TIMEOUT) -> Message:
    """Poll for a message from the consumer.

    This method differs from consumer.poll in that it will never return a null message
    or a message that has an error. If you set timeout and poll hits that timeout,
    it will raise a TimeoutError.

    Args:
        consumer (Consumer): Consumer that polls for messages.
        timeout (Optional[float], optional): Seconds to wait for a message before giving up.
            If `None`, block indefinitely.
            Defaults to DEFAULT_POLL_TIMEOUT.

    Raises:
        TimeoutError: If no messages are retrieved in the specified timeout.
        ValueError: If timeout is not positive.

    Returns:
        Message: A Kafka message.
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

        with bind_message_ctx(message):
            LOGGER.debug(
                "Received message -- topic=%s partition=%s offset=%s",
                message.topic(),
                message.partition(),
                message.offset(),
            )

            if message.error():
                # If we cannot read the raw message correctly, we give up and
                # raise the error to the caller.
                raise ConsumerException(f"Got bad message from Kafka: {message.error()}")

        return message


@contextmanager
def bind_message_ctx(message: Message, **additional_ctx) -> Iterator[None]:
    """Add kafka message context to muselog logs.

    Args:
        message (Message): The Kafka Message.
        additional_ctx: Any additional context you wish to bind.
    """
    ctx = dict(
        kafka_topic=message.topic(),
        kafka_partition=message.partition(),
        kafka_offset=message.offset(),
    )
    ctx.update(additional_ctx)
    muselog.context.bind(**ctx)
    try:
        yield
    finally:
        # Clear only our context.
        # This will clear the `ctx` keys even if their values have been overwritten
        # by our yieldee.
        muselog.context.unbind(*list(ctx.keys()))
