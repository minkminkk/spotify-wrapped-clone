import os


def test_env():
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        error_msg = "Correct SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET " \
            + "must be set to perform complete tests"
        raise EnvironmentError(error_msg)
    
    
test_env()