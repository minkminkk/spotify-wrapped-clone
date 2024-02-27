import pytest
from faker import Faker
from include.faker_custom_providers import spotify, user_info, user_clickstream


@pytest.fixture(scope = "module")
def fake():
    fake = Faker()
    Faker.seed(0)
    fake.add_provider(spotify.Provider)
    fake.add_provider(user_info.Provider)
    fake.add_provider(user_clickstream.Provider)
    return fake


@pytest.fixture
def user_profile(fake):
    return fake.user_profile()


def test_attrs(user_profile):
    # Do not need value test because
    # Test for _id value already included in test_spotify.test_user_id
    # All other fields come in Faker's profile standard provider
    # (https://faker.readthedocs.io/en/master/providers/faker.providers.profile.html)
    attrs = set(
        [
            "_id", # test for this field 
            "username", 
            "name", 
            "sex", 
            "address", 
            "mail", 
            "birthdate"
        ]   # 
    )
    assert set(attrs) == set(user_profile.keys())