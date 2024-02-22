import pytest
import os


@pytest.fixture(scope = "session")
def client_id():
    return os.getenv("SPOTIFY_CLIENT_ID")


@pytest.fixture(scope = "session")
def client_secret():
    return os.getenv("SPOTIFY_CLIENT_SECRET")


def test_env():
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        error_msg = "Correct SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET " \
            + "must be set to perform complete tests"
        raise EnvironmentError(error_msg)
    
    
test_env()