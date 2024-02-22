import pytest
from utils.spotify_api_client.session import *
import os


client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")


@pytest.fixture(scope = "session")
def client_id():
    return os.getenv("SPOTIFY_CLIENT_ID")
    

@pytest.fixture(scope = "session")
def client_secret():
    return os.getenv("SPOTIFY_CLIENT_SECRET")


def test_creation_using_env():
    # Test successful creation
    SpotifyAPISession()


def test_creation_using_args(client_id, client_secret):
    # Test successful creation
    SpotifyAPISession(client_id, client_secret)


def test_invalid_creation():
    # Raises EnvironmentError if instatiated without environment vars or inputs
    if os.environ["SPOTIFY_CLIENT_ID"]:
        del os.environ["SPOTIFY_CLIENT_ID"]
    if os.environ["SPOTIFY_CLIENT_SECRET"]:
        del os.environ["SPOTIFY_CLIENT_SECRET"]

    with pytest.raises(EnvironmentError):
        SpotifyAPISession()


def test_context_manager(client_id, client_secret):
    # Test __enter__
    with SpotifyAPISession(client_id, client_secret) as s:
        assert hasattr(s, "client_token")
        assert hasattr(s, "http_session")

    # Test __exit__
    assert not hasattr(s, "client_token")
    assert not hasattr(s, "http_session")