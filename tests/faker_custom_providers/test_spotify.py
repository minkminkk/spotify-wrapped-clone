import pytest
from faker import Faker
from packages.faker_custom_providers import spotify, user_info, user_clickstream
import string


@pytest.fixture(scope = "module")
def fake():
    fake = Faker()
    Faker.seed(0)
    fake.add_provider(spotify.Provider)
    fake.add_provider(user_info.Provider)
    fake.add_provider(user_clickstream.Provider)
    return fake


def assert_entity_id(id):
    # entity_id must be 22 hexadecimal chars
    assert all([c in string.hexdigits for c in id])


def test_user_id(fake):
    for _ in range(10): # avoid false positive tests
        user_id = fake.user_id()
        assert_entity_id(user_id)


def test_track_id(fake):
    for _ in range(10): # avoid false positive tests
        track_id = fake.track_id()
        assert_entity_id(track_id)