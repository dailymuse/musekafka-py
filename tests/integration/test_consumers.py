from typing import Iterator, List

import pytest
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from musekafka import consumers

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


@pytest.fixture
def app(broker_host: str, registry_url: str, topics: List[str]) -> Iterator[consumers.App]:
    consumer_app = consumers.App(
        "testconsumers",
        [broker_host],
        topics,
        consumer_cls=AvroConsumer,
        schema_registry_url=registry_url,
    )
    yield consumer_app
    consumer_app.close()


def test_app_stream(app: consumers.App, producer: SerializingProducer, topics: List[str]):
    """App.stream consumes all data up to count."""
    for topic in topics:
        producer.produce(topic, value={"test": "TESTING!1"})
        producer.produce(topic, value={"test": "TESTING!2"})
        # Expect default to kick in.
        producer.produce(topic, value={})

    with app.stream(count=3) as stream:
        messages = list(stream)

    assert len(messages) == 3
    assert messages[0].value() == {"test": "TESTING!1"}
    assert messages[1].value() == {"test": "TESTING!2"}
    assert messages[2].value() == {"test": None}


def test_app_batch(app: consumers.App, producer: SerializingProducer, topics: List[str]):
    """App.batch consumes a batch of messages."""
    for topic in topics:
        for i in range(4):
            producer.produce(topic, value={"test": f"TESTING!{i}"})

    with app.batch(batch_size=2, batch_count=2) as batch:
        first_batch, second_batch = list(batch)

    assert [m.value() for m in first_batch] == [{"test": "TESTING!0"}, {"test": "TESTING!1"}]
    assert [m.value() for m in second_batch] == [{"test": "TESTING!2"}, {"test": "TESTING!3"}]


def test_app_stream_fail(app: consumers.App, producer: SerializingProducer, topics: List[str]):
    """App.stream exits on exception, and does not commit offsets."""
    expected_messages_pre = [{"test": f"TESTING!{i}"} for i in range(4)]
    fail_message = {"test": "FAIL."}
    expected_messages_post = [{"test": f"TESTING AFTER!{i}"} for i in range(4)]
    for topic in topics:
        for message in expected_messages_pre:
            producer.produce(topic, value=message)
        # BAD MESSAGE
        producer.produce(topic, value=fail_message)
        # We should not consume these messages until
        # the final call to stream that does not result in an exception.
        for message in expected_messages_post:
            producer.produce(topic, value=message)

    actual_messages = []
    with pytest.raises(SystemExit):
        with app.stream(close=False, timeout=5) as stream:
            for msg in stream:
                if msg.value()["test"] == "FAIL.":
                    raise Exception("Dun goofed.")
                actual_messages.append(msg.value())

    # Only got the first four messages before we enocuntered an error.
    assert actual_messages == expected_messages_pre

    with pytest.raises(SystemExit):
        with app.stream(close=False, timeout=5) as stream:
            for msg in stream:
                if msg.value()["test"] == "FAIL.":
                    raise Exception("Dun goofed.")
                actual_messages.append(msg.value())

    # Should not have received any additional messages.
    assert actual_messages == expected_messages_pre

    # Let's just consume the remaining messages to check that
    # we do consume everything.
    with app.stream(count=len(expected_messages_post) + 1, timeout=5) as stream:
        for msg in stream:
            actual_messages.append(msg.value())

    assert actual_messages == expected_messages_pre + [fail_message] + expected_messages_post


def test_app_batch_fail(app: consumers.App, producer: SerializingProducer, topics: List[str]):
    """App.batch exits on exception, and does not commit offsets."""
    expected_messages_pre = [{"test": f"TESTING!{i}"} for i in range(4)]
    fail_message = {"test": "FAIL."}
    expected_messages_post = [{"test": f"TESTING AFTER!{i}"} for i in range(4)]
    for topic in topics:
        for message in expected_messages_pre:
            producer.produce(topic, value=message)
        # BAD MESSAGE
        producer.produce(topic, value=fail_message)
        # We should not consume these messages until
        # the final call to stream that does not result in an exception.
        for message in expected_messages_post:
            producer.produce(topic, value=message)

    actual_messages = []
    with pytest.raises(SystemExit):
        with app.batch(close=False, batch_size=4, timeout=5) as batch:
            for msgs in batch:
                for msg in msgs:
                    if msg.value()["test"] == "FAIL.":
                        raise Exception("Dun goofed.")
                    actual_messages.append(msg.value())

    # Only got the first four messages before we enocuntered an error.
    assert actual_messages == expected_messages_pre

    with pytest.raises(SystemExit):
        with app.batch(close=False, batch_size=4, timeout=5) as batch:
            for msgs in batch:
                for msg in msgs:
                    if msg.value()["test"] == "FAIL.":
                        raise Exception("Dun goofed.")
                    actual_messages.append(msg.value())

    # Should not have received any additional messages.
    assert actual_messages == expected_messages_pre

    # Let's just consume the remaining messages to check that
    # we do consume everything.
    with app.batch(
        close=False, batch_size=1, batch_count=len(expected_messages_post) + 1, timeout=5
    ) as batch:
        for msgs in batch:
            for msg in msgs:
                actual_messages.append(msg.value())

    assert actual_messages == expected_messages_pre + [fail_message] + expected_messages_post
