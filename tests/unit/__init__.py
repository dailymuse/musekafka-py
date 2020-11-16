import time
from typing import Any, Optional


def make_side_effect(messages, delay=None):
    """Make a side effect from a list of messages, optionally adding a delay."""
    msg_queue = list(reversed(messages))
    sleep_delay = delay

    def side_effect(*args, **kwargs):
        if sleep_delay is not None:
            time.sleep(sleep_delay)
        return msg_queue.pop()

    return side_effect


class FakeMessage:
    """Implements the Message interface.

    You cannot create messages directly (C code does not expose init method),
    so we need a fake. Again, I will extend to fill out the interface as needed.
    """

    def __init__(
        self,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        error: Optional[str] = None,
        value: Any = None,
    ) -> None:
        """Create the fake message."""
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._error = error
        self._value = value

    def topic(self) -> str:
        """Return the topic this message is from."""
        return self._topic

    def partition(self) -> int:
        """Return the partition for this message."""
        return self._partition

    def offset(self) -> int:
        """Return the offset of this message."""
        return self._offset

    def error(self) -> Optional[str]:
        """Return an error associated with the message, if any."""
        return self._error

    def value(self) -> Any:
        """Return the value set for the message."""
        return self._value
