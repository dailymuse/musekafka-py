import contextlib
import logging
import sys
from typing import Any, Callable, Dict, Iterator, List, Optional, Type, TypeVar, Union

import muselog.context
from confluent_kafka import Consumer, Message, TopicPartition
from confluent_kafka.avro import AvroConsumer
from muselog.logger import get_logger_with_context

import musekafka.shutdown
from musekafka.exceptions import ConsumerException
from musekafka.message import batch as batch_messages
from musekafka.message import stream as stream_messages

LOGGER = get_logger_with_context(logging.getLogger(__name__))

DEFAULT_CONSUMER_CONFIG = {
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
    "enable.auto.offset.store": True,
    "enable.partition.eof": False,
}

MessageProviderResultT = TypeVar("MessageProviderResultT", List[Message], Message)


def default_on_assign(consumer: Consumer, partitions: List[TopicPartition]):
    """Log topic partition information when consumer gets its assignment.

    Args:
        consumer (Consumer): The assigned consumer.
        partitions (List[TopicPartition]): Topic partitions that the consumer has been assigned to.
    """
    musekafka.shutdown.exit_if_shutdown()
    LOGGER.info("Assigned consumer to the following partitions: %s", partitions)


def commit_processed_sync(consumer: Consumer, messages: Union[List[Message], Message]):
    """Synchronously commit all messages that have been processed.

    Args:
        consumer (Consumer): Consumer that will commit the messages.
        messages (Union[List[Message], Message]): Processed messages.

    Raises:
        ConsumerException: If one or more messages could not be committed.
    """
    topic_partitions = consumer.commit(asynchronous=False)
    errors = []
    for topic_partition in topic_partitions:
        if topic_partition.error is None:
            continue
        errors.append(
            f"topic={topic_partition.topic} partition={topic_partition.partition} error={str(topic_partition.error)}"  # noqa: E501
        )
    if errors:
        raise ConsumerException("Failed to commit one or more partitions:\n" + "\n\t".join(errors))


class App:
    """Consumer application."""

    def __init__(
        self,
        name: str,
        brokers: List[str],
        topics: List[str],
        schema_registry_url: Optional[str] = None,
        on_assign: Callable[[Consumer, List[TopicPartition]], None] = default_on_assign,
        on_revoke: Optional[Callable[[Consumer, List[TopicPartition]], None]] = None,
        commit_strategy: Callable[[Consumer, List[Message]], None] = commit_processed_sync,
        install_default_shutdown_handler: bool = True,
        consumer_cls: Type[Consumer] = Consumer,
        consumer_cfg: Dict[str, Any] = DEFAULT_CONSUMER_CONFIG,
        **extra_consumer_kwargs,
    ):
        """Create the consumer application.

        Args:
            name (str): Consumer name. Must be unique, as it is used as the consumer's group id.
            brokers (List[str]): Kafka Broker hostnames.
            topics (List[str]): Topics to consume from.
            schema_registry_url (Optional[str], optional): URL for the schema registry,
                if decoding messages according to a schema.
                Defaults to None.
            on_assign (Callable[[Consumer, List[TopicPartition]], None], optional): Callback to
                provide handling of customized offsets on completion of a successful
                partition re-assignment.
                Defaults to default_on_assign.
            on_revoke (Optional[Callable[[Consumer, List[TopicPartition]], None]], optional): Call-
                back to provide handling of offset commits to a customized store on the start of a
                rebalance operation.
                Defaults to None.
            commit_strategy (Callable[[Consumer, List[Message]], None], optional): Function to
                commit a message or message batch after it has been processed.
                Defaults to commit_processed_sync.
            install_default_shutdown_handler (bool, optional): If `True`, (re-)install the
                default shutdown handler. If `False`, you must manually install via
                musekafka.shutdown.install_shutdown_handler. This will have no
                effect if a handler is already present.
                Defaults to True.
            consumer_cls (Type[Consumer], optional): Consumer implementation.
                Defaults to Consumer.
            consumer_cfg (Dict[str, Any], optional): Consumer configuration.
                Defaults to DEFAULT_CONSUMER_CONFIG.
            extra_consumer_kwargs: Additional keyword arguments to pass to the underlying
                consumer instance.

        Raises:
            ValueError: If consumer_cls is an AvroConsumer subclass and schema_registry_url is
                `None`.
        """
        if issubclass(consumer_cls, AvroConsumer) and not schema_registry_url:
            raise ValueError("AvroConsumer and its subclasses require a schema registry url.")
        self.name = name
        self.topics = topics.copy()
        self.on_assign = on_assign
        self.on_revoke = on_revoke
        self.commit_strategy = commit_strategy
        effective_consumer_config = consumer_cfg.copy()
        effective_consumer_config["group.id"] = name
        effective_consumer_config["bootstrap.servers"] = ",".join(brokers)
        if schema_registry_url is not None:
            effective_consumer_config["schema.registry.url"] = schema_registry_url
        self.consumer = consumer_cls(effective_consumer_config, **extra_consumer_kwargs)
        self._subscribed = False
        if (
            install_default_shutdown_handler
            and not musekafka.shutdown.is_shutdown_handler_installed()
        ):
            musekafka.shutdown.install_shutdown_handler()

    def close(self):
        """Close the consumer."""
        self.consumer.close()

    @contextlib.contextmanager
    def stream(self, close: bool = True, **kwargs) -> Iterator[Iterator[Message]]:
        """Provide a stream of messages to the caller.

        Args:
            close (bool, optional): If `True`, close the consumer after all messages are consumed.
                Defaults to True.
            kwargs: Additional arguments to pass to the message streamer
                (see musekafka.message.stream).

        Yields:
            Iterator[Message]: Messages.
        """
        with self.consume(stream_messages, close=close, **kwargs) as messages:
            yield messages

    @contextlib.contextmanager
    def batch(self, close: bool = True, **kwargs) -> Iterator[Iterator[List[Message]]]:
        """Assemble consumed messages into batches and stream these batches to the caller.

        Args:
            close (bool, optional): If `True`, close the consumer after all messages are consumed.
                Defaults to True.
            kwargs: Additional arguments to pass to the message batcher
                (see musekafka.message.batch).

        Yields:
            Iterator[List[Message]]: Message batches.
        """
        with self.consume(batch_messages, close=close, **kwargs) as messages:
            yield messages

    @contextlib.contextmanager
    def consume(
        self,
        message_provider: Callable[..., Iterator[MessageProviderResultT]],
        close: bool = True,
        **kwargs,
    ) -> Iterator[Iterator[MessageProviderResultT]]:
        """Consume messages from the `message_provider`.

        Args:
            message_provider (Callable[..., Iterator[MessageProviderResultT]]): Function
                that returns a stream of message or message batches for processing.
            close (bool, optional): If `True`, close the consumer after all messages are consumed.
                Defaults to True.

        Yields:
            Iterator[MessageProviderResultT]: Consumed message(s) that are ready to be processed.
        """
        with self._bind_consumer_group_id():
            if not self._subscribed:
                try:
                    subscribe_kwargs = dict()
                    if self.on_assign is not None:
                        subscribe_kwargs["on_assign"] = self.on_assign
                    if self.on_revoke is not None:
                        subscribe_kwargs["on_revoke"] = self.on_revoke
                    self.consumer.subscribe(self.topics, **subscribe_kwargs)
                except Exception as e:
                    LOGGER.exception(str(e))
                    sys.exit("Consumer subscription failed.")
                else:
                    self._subscribed = True

            try:
                messages: Iterator[MessageProviderResultT] = message_provider(
                    self.consumer, **kwargs
                )

                def _msg_iterator() -> Iterator[MessageProviderResultT]:
                    for msg_or_batch in messages:
                        yield msg_or_batch
                        if self.commit_strategy is not None:
                            self.commit_strategy(self.consumer, msg_or_batch)
                        if musekafka.shutdown.is_shutting_down():
                            break

                yield _msg_iterator()
            except Exception as e:
                LOGGER.exception(str(e))
                sys.exit("Consumption failed.")
            finally:
                if close:
                    LOGGER.info("Closing consumer. This may take a moment....")
                    try:
                        self.close()
                    except Exception as e:
                        LOGGER.exception(str(e))
                        sys.exit("Unable to cleanly shutdown the consumer.")
                    LOGGER.info("Goodbye.")
                else:
                    # If we do not close the consumer, we need to re-subscribe
                    # to reset our position, otherwise subsequent calls to 'poll'
                    # may skip messages.
                    self.consumer.unsubscribe()
                    self._subscribed = False

    @contextlib.contextmanager
    def _bind_consumer_group_id(self) -> Iterator[None]:
        muselog.context.bind(kafka_consumer_group_id=self.name)
        try:
            yield
        finally:
            # Clear only our context.
            # This will clear the `ctx` keys even if their values have been overwritten
            # by our yieldee.
            muselog.context.unbind("kafka_consumer_group_id")
