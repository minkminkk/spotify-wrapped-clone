import pytest
from faker import Faker
from include.faker_custom_providers import spotify, user_info, user_clickstream
from datetime import datetime
import string


@pytest.fixture(scope = "module")
def fake():
    fake = Faker()
    Faker.seed(0)
    fake.add_provider(spotify.Provider)
    fake.add_provider(user_info.Provider)
    fake.add_provider(user_clickstream.Provider)
    return fake

@pytest.fixture
def test_dates():   # start_dt, end_dt
    return datetime(2018, 1, 1, 1), datetime(2018, 1, 1, 5)


def test_tracklist_for_user(fake):
    # Check when min_tracks > max_tracks
    with pytest.raises(ValueError):
        fake.tracklist_for_user(3, 2)

    # Check when min_tracks == max_tracks
    assert len(fake.tracklist_for_user(min_tracks = 0,  max_tracks = 0)) == 0
    assert len(fake.tracklist_for_user(min_tracks = 2, max_tracks = 2)) == 2
    
    # Check no duplicate tracks in resulting tracklist
    sample_track_ids = ["track" + str(n) for n in range(20)] # length 20
    track_sample = fake.tracklist_for_user(
        min_tracks = 10, 
        max_tracks = 15,
        sample_track_ids = sample_track_ids
    )
    assert len(set(track_sample)) == len(track_sample)

    # Check when min_tracks > number of sample track ids
    with pytest.raises(ValueError):
        fake.tracklist_for_user(
            min_tracks = 25, 
            max_tracks = 30, 
            sample_track_ids = sample_track_ids
        )


def test_events_from_one_user(fake):
    # Check cases of invalid input
    invalid_cases = [
        ("2018-01-01", "2018-01-02 00:00:00"),  # Invalid dt string format
        ("2018-13-01 00:00:00", "2018-13-02 00:00:00"), # invalid month field
        ("2018-01-31 00:00:00", "2018-01-32 00:00:00"), # invalid day field
        (datetime(2018, 1, 29), datetime(2018, 1, 28))  # start_dt > end_dt
    ]
    for sdt, edt in invalid_cases:
        with pytest.raises(ValueError):
            for _ in fake.events_from_one_user(sdt, edt, "_", 1):
                break
    
    # Every arg is specified
    kwargs = {
        "start_dt": datetime(2018, 1, 1, 1),
        "end_dt": datetime(2018, 1, 1, 5),
        "user_id": "_",
        "user_tracklist": ["track" + str(n) for n in range(3)],
        "max_events": 10
    }
    res = [_ for _ in fake.events_from_one_user(**kwargs)]

    # Check if generate over max_events
    assert len(res) <= kwargs["max_events"]
    
    # Check if user_id and track_ids is as specified
    assert all([event["user_id"] == "_" for event in res])
    assert all(
        [event["track_id"] in kwargs["user_tracklist"] for event in res]
    )

    # Case when user_id, user_tracklist, then max_events not specified
    del kwargs["user_id"]
    del kwargs["user_tracklist"]
    res = [_ for _ in fake.events_from_one_user(**kwargs)]
    del kwargs["max_events"]
    res.extend([_ for _ in fake.events_from_one_user(**kwargs)])

    cur_event_name = "play"
    for event in res:
        assert all([c in string.hexdigits for c in event["event_id"]]) \
            and len(event["event_id"]) == 64    # sha256 field
        assert kwargs["start_dt"] <= event["event_ts"] <= kwargs["end_dt"]
        assert event["event_name"] == cur_event_name
        assert all([c in string.hexdigits for c in event["track_id"]]) \
            and len(event["track_id"]) == 22    # 22 hexdigits

        cur_event_name = "stop" if cur_event_name == "play" else "play"


def test_events_from_users(fake):
    kwargs = {
        "start_dt": datetime(2018, 1, 1, 1),
        "end_dt": datetime(2018, 1, 1, 2),
        "user_id_list": [],
        "max_events_per_user": None
    }

    # Check when user_id_list is empty
    res = [_ for _ in fake.events_from_users(**kwargs)]
    assert len(res) == 0
    
    # Normal input - add user_id_list to input
    kwargs["user_id_list"] = [fake.user_id() for _ in range(5)]
        # generating valid user ids is done by Spotify provider
    res = [_ for _ in fake.events_from_users(**kwargs)]

    # Check if no new user_ids are generated within the method
    res_users = set({event["user_id"] for event in res})
    assert res_users == set(kwargs["user_id_list"])