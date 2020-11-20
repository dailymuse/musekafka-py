"""Demonstrates musekafka.consumer.App in stream mode.

To use, first start up the Kafka cluster using `make start-dev-kafka-cluster`.
Next, start this example by running `./devenv python examples/stream.py`.
Finally, use a tool such as kafkacat (https://github.com/edenhill/kafkacat) to send
messages to the broker.
"""

import docker
import muselog

from musekafka import consumers

muselog.setup_logging(module_log_levels={"musekafka": "DEBUG"})
docker_client = docker.from_env()
brokers = [
    f"localhost:{host['HostPort']}"
    for host in docker_client.api.port("musekafka-py_kafka_1", 9092)
]
app = consumers.App("stream_example", brokers, ["stream_example"])

with app.stream() as messages:
    for message in messages:
        print(message.value())
