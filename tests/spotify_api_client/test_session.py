import pytest
from spotify_api_client import auth, session


@pytest.fixture(scope = "module")
def authenticator() -> auth.ClientAuthenticator:
    return auth.ClientAuthenticator(
        "67420aa232044ea28373a5ac0f03f4c0",
        "ad75e799c0cd4d629782fc626feec7fa"
    )
    
@pytest.fixture(scope = "module")
def access_token(authenticator) -> dict:
    authenticator.set_strategy(auth.ClientCredentialsStrategy())
    return authenticator.get_access_token()

@pytest.fixture(scope = "module")
def spotify_session(access_token: dict):
    return session.APISession(access_token)


def test_valid_init(spotify_session: session.APISession):
    # Test successful creation
    assert hasattr(spotify_session, "root_url")
    assert "Authorization" in spotify_session.http_session.headers.keys()


def test_invalid_init():
    # Invalid token type
    # Do not test token field, let catch by response
    invalid_token = 100
    with pytest.raises(ValueError):
        session.APISession(invalid_token)

    invalid_token = "bar"
    with pytest.raises(ValueError):
        session.APISession(invalid_token)


def test_session_close(access_token: dict):
    s = session.APISession(access_token)
    assert "Authorization" in s.http_session.headers.keys()

    s.close()
    assert not hasattr(s, "http_session")


def test_session_context_manager(access_token: dict):
    with session.APISession(access_token) as s:
        assert "Authorization" in s.http_session.headers.keys()

    assert not hasattr(s, "http_session")


# TODO: Test get_genres(), search_tracks()