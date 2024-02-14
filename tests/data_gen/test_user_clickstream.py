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

@pytest.fixture
def metadata():   # default metadata
    return {
        "max_events_per_user": 4,
        "user_id_list": ["_1", "_2", "_3"]
    }


def test_tracklist_for_user(fake):
    with pytest.raises(ValueError):
        fake.tracklist_for_user(3, 2)

    assert len(fake.tracklist_for_user(0, 0)) == 0
    assert len(fake.tracklist_for_user(2, 2)) == 2
    assert 1 <= len(fake.tracklist_for_user(1, 3)) <= 3


def test_events_from_user_between(fake, test_dates):
    max_events_per_user = 4
    start_dt, end_dt = test_dates
    args = *test_dates, "_", max_events_per_user
    res = [_ for _ in fake.events_from_user_between(*args)]
    
    # Check cases of invalid input
    for sdt, edt in [
        ("2018-01-01", "2018-01-02 00:00:00"),  # Invalid dt string format
        ("2018-13-01 00:00:00", "2018-13-02 00:00:00"), # invalid month field
        ("2018-01-31 00:00:00", "2018-01-32 00:00:00"), # invalid day field
        (datetime(2018, 1, 29), datetime(2018, 1, 28))  # start_dt > end_dt
    ]:
        with pytest.raises(ValueError):
            for _ in fake.events_from_user_between(sdt, edt, "_", 1):
                break

    # Check if generate over max_events
    assert len(res) <= max_events_per_user

    # Check result fields
    cur_event_name = "play"
    for event in res:
        assert event["event_name"] == cur_event_name
        assert start_dt <= event["event_ts"] <= end_dt
        assert len(event["track_id"]) == 22

        cur_event_name = "stop" if cur_event_name == "play" else "play"


def test_events_from_users_between(fake, test_dates):
    # Check when user_id_list is empty
    res = [_ for _ in fake.events_from_users_between(*test_dates, [])]
    assert len(res) == 0
    
    # Normal input
    user_id_list = ["_1", "_2", "_3"]
    res = [_ for _ in fake.events_from_users_between(*test_dates, user_id_list)]

    # Check user count
    users_set = set(map(lambda x: x["user_id"], res))
    assert len(users_set) == len(user_id_list)