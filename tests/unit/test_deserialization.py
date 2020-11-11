from datetime import datetime

from musekafka import deserialization


def test_decode_none():
    """deserialization.decode_pg_dt returns None when given None."""
    assert deserialization.decode_pg_dt(None) is None


def test_decode_valid():
    """deserialization.decode_pg_dt returns a valid datetime."""
    # 1535648931950500 ~= 08/30/2018 5:08:51.950500 UTC
    micros = 1535648931950500
    dt = datetime(2018, 8, 30, hour=17, minute=8, second=51, microsecond=950500)
    assert deserialization.decode_pg_dt(micros) == dt
