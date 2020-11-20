"""Demonstrates musekafka.consumer.App in batch mode.

To use, first start up the Kafka cluster using `make start-dev-kafka-cluster`.
Next, start this example by running `./devenv python examples/batch.py`.
Finally, use a tool such as kafkacat (https://github.com/edenhill/kafkacat) to send
messages to the broker. For example:

Running:

echo "TEST batch1" | kafkacat -b localhost:9092 -t batch_example
echo "TEST batch2!" | kafkacat -b localhost:9092 -t batch_example

The consumer should output something similar to the following:

2020-11-13 03:30:05,912 - __main__:23 - INFO - My batch size: 2
2020-11-13 03:30:05,912 - __main__:25 - INFO - b'TEST batch1'
2020-11-13 03:30:05,912 - __main__:25 - INFO - b'TEST batch2!'

"""

import logging

import docker
import muselog
from muselog.logger import get_logger_with_context

from musekafka import consumers

muselog.setup_logging(root_log_level="INFO", module_log_levels={"musekafka": "DEBUG"})

docker_client = docker.from_env()
brokers = [
    f"localhost:{host['HostPort']}"
    for host in docker_client.api.port("musekafka-py_kafka_1", 9092)
]

LOGGER = get_logger_with_context(logging.getLogger(__name__))

app = consumers.App("batch_example", brokers, ["batch_example"])

with app.batch(batch_size=3, batch_timeout=5) as batch:
    for messages in batch:
        LOGGER.info("My batch size: %d", len(messages))
        for message in messages:
            LOGGER.info(message.value())
