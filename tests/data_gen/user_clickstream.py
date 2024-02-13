import pytest
from faker import Faker
from src.utils.custom_providers import user_clickstream
from datetime import datetime


@pytest.fixture
def fake():
    fake = Faker()
    fake.add_provider(user_clickstream.Provider)
    return fake


@pytest.fixture
def test_dates():   # start_dt, end_dt
    return datetime(2018, 1, 1, 1, 0, 0), datetime(2018, 1, 1, 5, 0, 0)


def test_events_per_user(fake, test_dates):
    max_events = 4
    start_dt, end_dt = test_dates
    res = [
        _ for _ in fake.events_per_user(
            max_events = max_events, 
            start_dt = start_dt, 
            end_dt = end_dt
        )
    ]

    # Check if generate over max_events
    assert len(res) <= max_events
    
    # Check result fields
    cur_event_name = "play"
    for event in res:
        assert event["event_name"] == cur_event_name
        assert start_dt <= event["event_ts"] <= end_dt
        assert len(event["track_id"]) == 22

        cur_event_name = "stop" if cur_event_name == "play" else "play"