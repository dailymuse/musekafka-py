"""Methods to assist with common deserialization tasks."""

from datetime import datetime, timedelta
from typing import Optional


def decode_pg_dt(pg_dt: Optional[int]) -> Optional[datetime]:
    """Convert the timezone-agnostic PostgreSQL timestamp to UTC datetime.

    Args:
        pg_dt (Optional[int]): The PostgreSQL timestamp integer, represented as an unix
            epoch with microsecond precision.

    Returns:
        Optional[datetime]: The Python datetime representation of the PostgreSQL timestamp.
    """
    if pg_dt is None:
        return None

    decoded_pg_dt = str(pg_dt)
    seconds, micros = decoded_pg_dt[:10], decoded_pg_dt[10:]
    return datetime.utcfromtimestamp(int(seconds)) + timedelta(microseconds=int(micros))
