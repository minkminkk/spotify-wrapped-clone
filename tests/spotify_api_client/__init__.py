import pytest
from utils.spotify_api_client import auth, session


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