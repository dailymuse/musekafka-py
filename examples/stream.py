"""Demonstrates musekafka.consumer.App in stream mode.

To use, first start up the Kafka cluster using `make start-dev-kafka-cluster`.
Next, start this example by running `./devenv python examples/stream.py`.
Finally, use a tool such as kafkacat (https://github.com/edenhill/kafkacat) to send
messages to the broker.
"""

import muselog

from musekafka import consumers

muselog.setup_logging(module_log_levels={"musekafka": "DEBUG"})

app = consumers.App("stream_example", ["localhost:9092"], ["stream_example"])
with app.stream() as messages:
    for message in messages:
        print(message.value())
