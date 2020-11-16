import musekafka


def test_version_exists():
    """musekafka.__version__ is set."""
    assert musekafka.__version__ is not None
