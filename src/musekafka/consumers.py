import contextlib
import logging
import sys
from typing import Any, Callable, Dict, Iterator, List, Optional, Type, TypeVar, Union

import muselog.context
from confluent_kafka import OFFSET_BEGINNING, Consumer, Message, TopicPartition
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
        consumer_cfg: Optional[Dict[str, Any]] = None,
        merge_default_consumer_cfg: bool = True,
        app_mode: bool = True,
        close_after_consume: bool = True,
        from_beginning: bool = False,
        topic_partitions: Optional[List[TopicPartition]] = None,
        timeout: Optional[float] = None,
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
            consumer_cfg (Optional[Dict[str, Any]], optional): Custom Consumer configuration.
                If `None`, use DEFAULT_CONSUMER_CFG.
                Defaults to None.
            merge_default_consumer_cfg (bool, optional): If `True`, merge consumer_cfg into
                DEFAULT_CONSUMER_CFG to create an effective configuration.
                If `False` (and consumer_cfg is set), rely completely on consumer_cfg.
                Defaults to True.
            app_mode (bool, optional): If `True`, exits the interpreter completely after
                a consumption round ends. If `False`, behavior depends on the *close_after_consume*
                setting. This setting is useful when running the consumer as a standalone
                application.
                Defaults to True.
            close_after_consume (bool, optional): If `True`, unsubscribe/unassign the consumer
                and reset all state. This operation is equivalent to creating a new instance.
                If `False`, the consumer will be paused after each round (i.e., stream or batch).
                On the next consumption round, the consumer will start back where it had been
                paused.
                Defaults to True.
            from_beginning (bool, optional): Tell the consumer to start at the
                beginning of the topic. This differs from 'auto.offset.reset="earliest"',
                as that will only apply to consumers with an unused group id.
                If you want that behavior, set auto.offset.reset="earliest" in
                the consumer_cfg.
                NOTE: This setting is not compatible with topic_partitions.
                WARNING: It is an error to use from_beginning while other consumers
                with the same group id are still running.
                Defaults to False.
            topic_partitions (Optional[List[TopicPartition]], optional): List of
                (topic, partition, offset) to assign the consumer to before it begins
                consumption.
                NOTE: This setting is not compatible with from_beginning.
                Defaults to None.
            timeout (Optional[float], optional): Maximum seconds consume operation is allowed
                by default.
                Defaults to None.
            extra_consumer_kwargs: Additional keyword arguments to pass to the underlying
                consumer instance.

        Raises:
            TypeError: If consumer_cls is an AvroConsumer subclass and schema_registry_url is
                `None`.
            TypeError: If both from_beginning and topic_partitions are set.
        """
        if issubclass(consumer_cls, AvroConsumer) and not schema_registry_url:
            raise ValueError("AvroConsumer and its subclasses require a schema registry url.")
        if from_beginning and topic_partitions:
            raise ValueError("from_beginning and topic_partitions are mutually exclusive.")
        self.name = name
        self.topics = topics.copy()
        self.on_assign = on_assign
        self.on_revoke = on_revoke
        self.commit_strategy = commit_strategy
        self.consumer_cls = consumer_cls
        effective_consumer_config = dict()
        if merge_default_consumer_cfg or not consumer_cfg:
            effective_consumer_config.update(DEFAULT_CONSUMER_CONFIG)
        elif consumer_cfg:
            effective_consumer_config.update(consumer_cfg)
        effective_consumer_config["group.id"] = name
        effective_consumer_config["bootstrap.servers"] = ",".join(brokers)
        if schema_registry_url is not None:
            effective_consumer_config["schema.registry.url"] = schema_registry_url
        self.consumer_cfg = effective_consumer_config
        self.consumer_kwargs = extra_consumer_kwargs
        self.consumer = self.build_consumer()
        self.app_mode = app_mode
        self.close_after_consume = close_after_consume
        self.from_beginning = from_beginning
        self.topic_partitions = topic_partitions
        self.timeout = timeout
        self.started = False
        self._assigned_topic_partitions: List[TopicPartition] = []

        if (
            install_default_shutdown_handler
            and not musekafka.shutdown.is_shutdown_handler_installed()
        ):
            musekafka.shutdown.install_shutdown_handler()

    def build_consumer(self, **consumer_cfg) -> Consumer:
        """Builds the Kafka consumer.

        Args:
            consumer_cfg: Additional / override configuration to pass to the consumer class.

        Returns:
            Consumer: The consumer.
        """
        cfg = self.consumer_cfg.copy()
        cfg.update(consumer_cfg)
        return self.consumer_cls(cfg, **self.consumer_kwargs)

    def start(self):
        """Startup the consumer."""
        if self.started:
            return

        if not self._assigned_topic_partitions:
            if self.from_beginning:
                for topic in self.topics:
                    topic_md = self.consumer.list_topics(topic=topic, timeout=5).topics[topic]
                    self._assigned_topic_partitions.extend(
                        TopicPartition(topic, part.id, OFFSET_BEGINNING)
                        for part in topic_md.partitions.values()
                    )
            elif self.topic_partitions:
                self._assigned_topic_partitions.extend(self.topic_partitions)

        if self._assigned_topic_partitions:
            self.consumer.assign(self._assigned_topic_partitions.copy())
        else:
            subscribe_kwargs = dict()
            if self.on_assign is not None:
                subscribe_kwargs["on_assign"] = self.on_assign
            if self.on_revoke is not None:
                subscribe_kwargs["on_revoke"] = self.on_revoke
            self.consumer.subscribe(self.topics, **subscribe_kwargs)
        self.started = True

    def stop(self):
        """Stop the consumer."""
        if not self.started:
            return
        if self._assigned_topic_partitions:
            self._assigned_topic_partitions = self.consumer.committed(
                self._assigned_topic_partitions, timeout=10
            )
            self.consumer.unassign()
        else:
            self.consumer.unsubscribe()
        self.started = False

    def close(self):
        """Close the consumer."""
        self.consumer.close()
        # We can't reuse the consumer after closing, so we need to create a new one.
        self.consumer = self.build_consumer()
        self.started = False
        self._assigned_topic_partitions.clear()

    @contextlib.contextmanager
    def stream(self, **kwargs) -> Iterator[Iterator[Message]]:
        """Provide a stream of messages to the caller.

        See musekafka.consumers.App.consume and musekafka.message.stream
        for arguments.

        Yields:
            Iterator[Message]: Messages.
        """
        with self.consume(stream_messages, **kwargs) as messages:
            yield messages

    @contextlib.contextmanager
    def batch(self, **kwargs) -> Iterator[Iterator[List[Message]]]:
        """Assemble consumed messages into batches and stream these batches to the caller.

        See musekafka.consumers.App.consume and musekafka.message.batch
        for arguments.

        Yields:
            Iterator[List[Message]]: Message batches.
        """
        with self.consume(batch_messages, **kwargs) as messages:
            yield messages

    @contextlib.contextmanager
    def consume(
        self,
        message_provider: Callable[..., Iterator[MessageProviderResultT]],
        close: Optional[bool] = None,
        **message_provider_kwargs,
    ) -> Iterator[Iterator[MessageProviderResultT]]:
        """Consume messages from the `message_provider`.

        Args:
            message_provider (Callable[..., Iterator[MessageProviderResultT]]): Function
                that returns a stream of message or message batches for processing.
            close (bool, optional): If `True`, close the consumer after all messages are consumed.
                If `None`, defer consumer app's *close_after_consume* setting.
                Defaults to None.
            message_provider_kwargs: Keyword arguments to pass to the *message_provider*.

        Yields:
            Iterator[MessageProviderResultT]: Consumed message(s) that are ready to be processed.
        """
        if close is None:
            close = self.close_after_consume
        is_sys_exit = False
        if "timeout" not in message_provider_kwargs:
            message_provider_kwargs["timeout"] = self.timeout
        with self._bind_consumer_group_id():
            try:
                self.start()
                messages: Iterator[MessageProviderResultT] = message_provider(
                    self.consumer, **message_provider_kwargs
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
                if not self.app_mode:
                    raise
                LOGGER.exception(str(e))
                is_sys_exit = True
                sys.exit("Consumption failed.")
            except SystemExit:
                is_sys_exit = True
                raise
            finally:
                if close or self.app_mode:
                    LOGGER.info("Closing consumer. This may take a moment....")
                    try:
                        self.close()
                    except Exception as e:
                        if not self.app_mode:
                            raise
                        LOGGER.exception(str(e))
                        sys.exit("Unable to cleanly shutdown the consumer.")
                    else:
                        LOGGER.info("Closed consumer.")
                    if self.app_mode and not is_sys_exit:
                        sys.exit()
                else:
                    # If we do not close the consumer, we need to stop it,
                    # otherwise subsequent calls to 'poll' may skip messages.
                    self.stop()

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
