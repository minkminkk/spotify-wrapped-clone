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


def check_status_code(response: requests.Response):
    """Preliminary checks for responses from Spotify API server.
    Raise exceptions when checks failed.

    :param response: response from Spotify server
    :type response: requests.Response
    :raises requests.HTTPError: at 4xx-5xx status codes. 
        Same as requests.raise_for_status() but with custom error messages
    """
    if 400 <= response.status_code < 600:
        server_msg = response.json()["error"]

        if 400 <= response.status_code < 500:
            error_msg = f"{response.status_code} Client Error: {server_msg}"
            raise requests.HTTPError(error_msg)
        else:
            error_msg = f"{response.status_code} Server Error: {server_msg}"
            raise requests.HTTPError(error_msg)