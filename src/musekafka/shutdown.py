"""This module houses helpers to implement safe shutdown of consumers.

Module logic applies to all consumers executing in the current python environment,
on the main thread.
"""

import signal
import threading
from typing import Tuple

# Default signals that trigger a shutdown event.
DEFAULT_SHUTDOWN_SIGNALS = (signal.SIGINT, signal.SIGTERM)

# Indicate that we need to shutdown.
SHUTDOWN = threading.Event()

# Indicate that the shutdown handler has been installed
SHUTDOWN_HANDLER_INSTALLED = threading.Event()


def exit_if_shutdown():
    """Raise SystemExit if shutdown handler has been called."""
    if SHUTDOWN.is_set():
        raise SystemExit


def is_shutting_down() -> bool:
    """Indicate that the shutdown handler has been called.

    Returns:
        bool: `True` if the shutdown handler has been called. `False` otherwise.
    """
    return SHUTDOWN.is_set()


def install_shutdown_handler(signals: Tuple[int, ...] = DEFAULT_SHUTDOWN_SIGNALS):
    """Install signal handlers that support graceful shutdown of consumers.

    Args:
        signals (Tuple[int, ...], optional): Signals to treat as shutdown indicators.
            Defaults to DEFAULT_SHUTDOWN_SIGNALS.
    """
    for sig in signals:
        signal.signal(sig, _shutdown)
    SHUTDOWN_HANDLER_INSTALLED.set()


def is_shutdown_handler_installed() -> bool:
    """Indicate if a shutdown handler has been installed.

    Returns:
        bool: `True` if a handler has been installed. `False` otherwise.
    """
    return SHUTDOWN_HANDLER_INSTALLED.is_set()


def _shutdown(_signum, _frame):
    SHUTDOWN.set()
