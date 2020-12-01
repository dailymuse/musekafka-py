# musekafka-py

Kafka consumer library that makes it easier to work with [Confluent Kafka Python](https://docs.confluent.io/5.0.0/clients/confluent-kafka-python/index.html)'s Consumer construct.

## Installation

```
# requirements.txt
--extra-index-url https://pypi.fury.io/themuse/

musekafka==0.2.1
```

## Usage

See [examples](examples) for examples.
See [consumers](src/musekafka/consumers.py) for the application api
and [message](src/musekafka/message.py) for the low-level apis.
We will have properly generated API documentation soon.

## Development

### Pre-reqs

-   python >= 3.8
-   docker >= 19 (for integration tests)
-   docker-compose >= 1.25 (for integration tests)

We provide [vscode](https://code.visualstudio.com/) configuration.

### Setup

Run `make setup` to setup the development environment.
The command does the following:

-   Installs tox if it is not already installed
-   Configures tox [development environment](tox.ini)
-   Installs [pre-commit](https://pre-commit.com/). See [.pre-commit-config.yaml](.pre-commit-config.yaml) for the git hooks that we use.

### Interact

Musekafka runs inside a [tox](https://tox.readthedocs.io/en/latest/)-managed virtual environment.
For your convenience, we provide [devenv](devenv), which you may use to execute commands in the virtual environment.
E.g., to run just the [deserialization](tests/unit/test_deserialization.py) unit tests:

```
./devenv pytest tests/unit/test_deserialization.py
```

You may also source the file to activate the virtual environment in the current terminal, e.g.,

```
. devenv
pytest tests/unit/test_deserialization.py
```

We provide a local Kafka and schema registry cluster both for testing and development
purposes. Start up this cluster by running `docker-compose up -V -d`. It is important
that you use `-V`, because otherwise Kafka and Schema Registry will advertise listeners
that are inconsistent with the listeners stored in ZooKeeper from a previous run.

On startup, Kafka will create the `develop` topic for you to use. You will need to configure
musekafka for the right broker host. You will also need to know the schema registry host if using schemata.
To avoid the need for a docker development container, the broker and schema registry containers bind
to a random port on localhost. You can find the host:port binding using `docker-compose port registry 8081` and `docker-compose port kafka 9092` for the Schema Registry and Kafka Broker, respectively. Given `0.0.0.0:40933`
for the broker and `0.0.0.0:40934` for the schema registry, you would configure a musekafka App as follows.

```
from musekafka import consumers

topics = ["develop"]
brokers = ["localhost:40933"]
schema_registry_url = "http://localhost:40934"
app = consumers.App("musekafka_develop", brokers, topics, schema_registry_url=schema_registry_url)
```

Then you may use the App api, as described in [consumers](src/musekafka/consumers.py).

If you are working with a lower level API, you would need to construct and subscribe a Confluent consumer directly,
like so:

```
from confluent_kafka.avro import AvroConsumer

consumer = AvroConsumer(
    {
        "group.id": "musekafka_develop",
        "bootstrap.servers": "localhost:40933",
        "schema.registry.url": "http://localhost:40934",
        ...
    }
)

consumer.subscribe(["develop"])
```

### Testing

Run `make test` to run all tests.
Run `make unit-test` to run unit tests. These tests are run by pre-commit for each `git commit`.
Run `make integration-test` to run integration tests.
The integration tests depend on a local Kafka cluster.
This is managed by docker, docker-compose, and pytest-docker.
They may take a while, so we suggest you run them before you push to remote rather
than on every local commit.

We use [pytest](https://docs.pytest.org/en/stable/). You can run individual test files like so:

```
./devenv pytest tests/unit/test_deserialization.py
```

You can also run individual tests:

```
./devenv pytest tests/unit/test_deserialization.py::test_decode_valid
```
