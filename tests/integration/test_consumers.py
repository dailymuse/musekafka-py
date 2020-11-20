from typing import Iterator, List

import pytest
from confluent_kafka import OFFSET_BEGINNING, SerializingProducer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from musekafka import consumers
from musekafka.message import stream as message_stream

GOOD_SCHEMA = """
{
  "type": "record",
  "name": "Test",
  "namespace": "musekafka.test",
  "fields": [
      {
          "name": "test",
          "type": ["null", "string"],
          "default": null
      }
  ]
}
"""


class Goofed(Exception):
    """Marker exception to assert test success."""


@pytest.fixture(scope="module")
def producer(broker_host: str, registry: SchemaRegistryClient) -> SerializingProducer:
    return SerializingProducer(
        {
            "bootstrap.servers": broker_host,
            "value.serializer": AvroSerializer(GOOD_SCHEMA, registry),
        }
    )


@pytest.fixture(scope="module")
def topics(
    broker_host: str, admin: AdminClient, registry: SchemaRegistryClient
) -> Iterator[List[str]]:
    # Seed data for the tests. We do this up-front, as otherwise the tests
    # will be too slow.
    topic_futures = admin.create_topics(
        [NewTopic("musekafka_consumer_test", 1, 1)], operation_timeout=20
    )

    for _, future in topic_futures.items():
        future.result()  # Block until the topic gets created.

    topics = list(topic_futures.keys())
    yield topics
    admin.delete_topics(topics, operation_timeout=20)
    subjects = [f"{topic}-value" for topic in topics]
    for subj in registry.get_subjects():
        if subj in subjects:
            registry.delete_subject(subj)


@pytest.fixture(scope="module")
def messages(producer: SerializingProducer, topics: List[str]) -> List[dict]:
    test_messages = [{"test": f"TESTING!{i}"} for i in range(4)]
    test_messages.append({"test": "FAIL."})
    test_messages.extend([{"test": f"TESTING AFTER!{i}"} for i in range(4)])
    for topic in topics:
        for message in test_messages:
            producer.produce(topic, value=message)
    return test_messages


@pytest.fixture
def app(broker_host: str, registry_url: str, topics: List[str]) -> consumers.App:
    consumer_app = consumers.App(
        "testconsumers",
        [broker_host],
        topics,
        consumer_cls=AvroConsumer,
        schema_registry_url=registry_url,
        app_mode=False,
        from_beginning=True,
        close_after_consume=False,
        timeout=10,
    )
    yield consumer_app
    consumer_app.close()


def test_app_stream(app: consumers.App, messages: List[dict]):
    """App.stream consumes all data up to count."""
    with app.stream(count=len(messages)) as stream:
        actual_messages = [msg.value() for msg in stream]

    assert actual_messages == messages


def test_app_batch(app: consumers.App, messages: List[dict]):
    """App.batch consumes a batch of messages."""
    with app.batch(batch_size=2, batch_count=2) as batches:
        first_batch, second_batch = list(batches)

    assert [m.value() for m in first_batch] == messages[:2]
    assert [m.value() for m in second_batch] == messages[2:4]


def test_app_stream_fail(app: consumers.App, messages: List[dict]):
    """App.stream exits on exception, and does not commit offsets."""
    actual_messages = []
    with pytest.raises(Goofed):
        with app.stream() as stream:
            for msg in stream:
                if msg.value()["test"] == "FAIL.":
                    raise Goofed("Dun goofed.")
                actual_messages.append(msg.value())

    # Only got the first four messages before we enocuntered an error.
    fail_msg_idx = messages.index({"test": "FAIL."})
    assert actual_messages == messages[:fail_msg_idx]

    with pytest.raises(Goofed):
        with app.stream() as stream:
            for msg in stream:
                if msg.value()["test"] == "FAIL.":
                    raise Goofed("Dun goofed.")
                actual_messages.append(msg.value())

    # Should not have received any additional messages.
    assert actual_messages == messages[:fail_msg_idx]

    # Let's just consume the remaining messages to check that
    # we do consume everything.
    with app.stream(count=len(messages[fail_msg_idx:]) + 1) as stream:
        for msg in stream:
            actual_messages.append(msg.value())

    assert actual_messages == messages


def test_app_batch_fail(app: consumers.App, messages: List[dict]):
    """App.batch exits on exception, and does not commit offsets."""
    actual_messages = []
    with pytest.raises(Goofed):
        with app.batch(batch_size=4) as batch:
            for msgs in batch:
                for msg in msgs:
                    if msg.value()["test"] == "FAIL.":
                        raise Goofed("Dun goofed.")
                    actual_messages.append(msg.value())

    # Only got the first four messages before we enocuntered an error.
    fail_msg_idx = messages.index({"test": "FAIL."})
    assert actual_messages == messages[:fail_msg_idx]

    with pytest.raises(Goofed):
        with app.batch(batch_size=4) as batch:
            for msgs in batch:
                for msg in msgs:
                    if msg.value()["test"] == "FAIL.":
                        raise Goofed("Dun goofed.")
                    actual_messages.append(msg.value())

    # Should not have received any additional messages.
    assert actual_messages == messages[:fail_msg_idx]

    # Let's just consume the remaining messages to check that
    # we do consume everything.
    with app.batch(batch_size=1, batch_count=len(messages[fail_msg_idx:]) + 1) as batch:
        for msgs in batch:
            for msg in msgs:
                actual_messages.append(msg.value())

    assert actual_messages == messages


def test_app_consume_from_end(
    app: consumers.App, topics: List[str], producer: SerializingProducer, messages: List[dict]
):
    """App.consume consumes from the tail end of the topic on first startup."""
    app.from_beginning = False
    actual_messages = []

    with app.consume(message_stream, timeout=0.5) as msgs:
        actual_messages.extend(msgs)

    # Will not have consumed anything, since we are tailing.
    assert not actual_messages

    # Now we add an additional message. We should consume it.
    for topic in topics:
        producer.produce(topic, value={"test": "An additional message!"})

    with app.consume(message_stream, count=1) as msgs:
        actual_messages.extend([msg.value() for msg in msgs])

    assert actual_messages == [{"test": "An additional message!"}]


def test_app_consume_topic_partitions(app: consumers.App, topics: List[str], messages: List[dict]):
    """App.consume consumes starting from the given topic partitions."""
    offset = len(messages) - 3
    app.from_beginning = False
    app.topic_partitions = [TopicPartition(topic, 0, offset) for topic in topics]

    expected_messages = messages[offset:]
    actual_messages = []

    with app.consume(message_stream, count=len(expected_messages)) as msgs:
        actual_messages.extend([msg.value() for msg in msgs])

    assert actual_messages == expected_messages


def test_app_mode(app: consumers.App):
    """App.consume raises SystemExit in app mode."""
    app.app_mode = True
    with pytest.raises(SystemExit):
        with app.consume(message_stream, count=1) as msgs:
            list(msgs)


def test_app_mode_exception(app: consumers.App, messages: List[dict]):
    """App.consume raises SystemExit in app mode if an underlying exception occurs."""
    app.app_mode = True
    with pytest.raises(SystemExit):
        with app.consume(message_stream, count=len(messages)) as msgs:
            for msg in msgs:
                if msg.value()["test"] == "FAIL.":
                    raise Goofed("Dun goofed.")


def test_app_start_for_assign(app: consumers.App):
    """App.start assigns the consumer."""
    app.start()
    assert app.started
    assert len(app.consumer.assignment()) == 1


def test_app_start_for_subscribe(
    app: consumers.App, topics: List[str], producer: SerializingProducer
):
    """App.start subscribes the consumer."""

    def on_assign(consumer, _partitions):
        for topic in topics:
            producer.produce(topic, value={"test": "Assigned."})

    app.from_beginning = False
    # on_assign is only called for subscribe, so this is a good
    # check that we subscribed rather than made an explicit assignment.
    app.on_assign = on_assign
    app.start()
    assert app.started, "consumer not started."

    success = False
    for msg in message_stream(app.consumer, timeout=20):
        if msg.value()["test"] == "Assigned.":
            success = True
            break
    assert success, "consumer not assigned."


def test_app_stop_for_assign(app: consumers.App):
    """App.stop unassigns the consumer."""
    # Need to start consumer for stop to have any impact.
    app.start()
    # assign will block, so we can immediately call stop.
    app.stop()
    assert not app.started, "app is still running."
    assert not app.consumer.assignment(), "consumer still has an assignment"


def test_app_stop_for_subscribe(
    app: consumers.App, topics: List[str], producer: SerializingProducer
):
    """App.stop unassigns the consumer."""
    subscribed = False
    # Need to start consumer for stop to have any impact.

    def on_assign(_consumer, _partitions):
        nonlocal subscribed
        subscribed = True
        for topic in topics:
            producer.produce(topic, value={"test": "Assigned."})

    def on_revoke(consumer, partitions):
        for topic in topics:
            consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
            producer.produce(topic, value={"test": "Revoked."})

    # setting from_beginning to False will put us in subscribe mode.
    app.from_beginning = False
    app.on_assign = on_assign
    app.on_revoke = on_revoke
    app.start()

    for msg in message_stream(app.consumer, timeout=20):
        # Don't really care which message we get. Just need something.
        break
    assert subscribed, "consumer never got subscribed."

    app.stop()
    assert not app.started, "app is still running."

    for msg in message_stream(app.consumer, timeout=20):
        # Don't really care which message we get. Just need something.
        if msg.value() == {"test": "Revoked."}:
            subscribed = False
            break

    assert not subscribed, "consumer is still subscribed."
