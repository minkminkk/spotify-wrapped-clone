import pytest
from faker import Faker
from faker_custom_providers import spotify, user_info, user_clickstream
import string


@pytest.fixture(scope = "module")
def fake():
    fake = Faker()
    Faker.seed(0)
    fake.add_provider(spotify.Provider)
    fake.add_provider(user_info.Provider)
    fake.add_provider(user_clickstream.Provider)
    return fake

@pytest.fixture(scope = "module")
def user_profile(fake):
    return [fake.user_profile() for _ in range(100)]


# Do not need value test because
# Test for user_id value already included in test_spotify.test_user_id
# All other fields come in Faker's profile standard provider
# (https://faker.readthedocs.io/en/master/providers/faker.providers.profile.html)
def test_attrs(user_profile):
    attrs = set(
        [
            "user_id", # no need tests for value of this field 
            "username", 
            "name", 
            "sex", 
            "address", 
            "mail", 
            "birthdate"
        ]
    )
    for profile in user_profile:
        assert set(attrs) == set(profile.keys())