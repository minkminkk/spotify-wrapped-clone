import requests
        

def check_response_json(response: requests.Response):
    """Check if response contains JSON object. Raise exceptions if not.

    :param response: response from Spotify server
    :type response: requests.Response
    :raises Exception: when response does not contain JSON object
    """
    if "application/json" not in response.headers.get("content-type", ""):
        error_msg = f"Response is not in JSON format from URL {response.url}"
        raise Exception(error_msg)