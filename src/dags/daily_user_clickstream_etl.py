#TODO: 
# - Use MongoClient and utils.faker_custom_provider to ingest data into MongoDB
# - Use Spotify tracks dataset to get available track_ids 
# and use it as sample for faking data
# (https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset)
#       - Download dataset and use Airflow to save track_id column as another file
#       - When generating data, read data from that file

from typing import List
import pendulum

from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.decorators import dag, task
from airflow.decorators.setup_teardown import setup_task, teardown_task
from airflow.models.variable import Variable


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": 3
}


@dag(
    dag_id = "daily_user_clickstream_etl",
    schedule = "@daily",
    start_date = pendulum.datetime(2018, 1, 1, tz = "UTC"),
    tags = ["etl"]
)
def daily_user_clickstream_etl():
    import logging
    import tenacity


    @task
    def get_client_credentials():
        from airflow.models.variable import Variable
        import base64
        import requests

        # Check client variables
        client_id = Variable.get("spotify_webapi_client_id", None)
        client_secret = Variable.get("spotify_webapi_client_secret", None)

        if not client_id or not client_secret:
            error_msg = "`spotify_webapi_client_id` and/or " \
                + "`spotify_webapi_client_secret` Airflow variables not found"
            raise EnvironmentError(error_msg)

        # Prepare auth msg in header in base64 (as specified in API docs)
        auth_str = f"{client_id}:{client_secret}"
        auth_bytes = auth_str.encode("utf-8")
        auth_b64 = base64.b64encode(auth_bytes)
        auth_msg = auth_b64.decode("utf-8")

        response = requests.post(
            url = "https://accounts.spotify.com/api/token", 
            data = {"grant_type": "client_credentials"},
            headers = {
                "Authorization": f"Basic {auth_msg}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )

        try:
            response.raise_for_status()
        except requests.HTTPError:
            self.log.error("Failed to get client token")
            raise
        return response.json()
    client_token = get_client_credentials()


    @task
    def get_auth_headers(token):
        logging.info("Building authentication message from token")
        return {
            "Authorization": token["token_type"] + " " + token["access_token"]
        }
    auth_headers = get_auth_headers(client_token)
    

    http_hook = HttpHook(method = "GET", http_conn_id = "http_spotify_webapi")
    # retry_args = {
    #     "wait": tenacity.wait_exponential(),
    #     "stop": tenacity.stop_after_attempt(10),
    #     "retry": tenacity.retry_if_exception_type(Exception)
    # }

    @task
    def get_genres(auth_headers):
        endpoint = "v1/recommendations/available-genre-seeds"

        logging.info("Requesting to url " + http_hook.url_from_endpoint(endpoint))
        response = http_hook.run(
            endpoint = endpoint, 
            headers = auth_headers, 
            extra_options = {"check_response": True}
        )

        return response.json()["genres"]
    genres = get_genres(auth_headers)

    
    def get_next_pages(r_json: dict):
        for category in r_json.keys():
            next_url = r_json[category]["next"]
            
            if next_url:
                yield next_url


    @task
    def search_tracks(genres, auth_headers):
        from queue import Queue
        import requests

        http_session = http_hook.get_conn(headers = auth_headers)
        endpoint = "v1/search"
        tracks = []

        # Put initial URLs into queue - process by genre
        for g in genres:
            url_q = Queue()
            params = {
                "q": "genre:" + g,
                "type": "track",
                "limit": 50,
                "offset": 0
            }

            url = requests.Request(
                url = http_hook.url_from_endpoint(endpoint),
                params = params
            ).prepare().url

            url_q.put_nowait(url)

            while not url_q.empty():
                url = url_q.get_nowait()
                
                logging.info("Requesting to url " + url)
                response = http_session.get(url)
                http_hook.check_response(response)
                r_json = response.json()

                
                for category in r_json.keys():
                    tracks.extend(r_json[category]["items"])

                    next_url = r_json[category]["next"]
                    if next_url:
                        url_q.put_nowait(next_url)

        return tracks

    tracks = search_tracks(genres, auth_headers)

    # TODO: Decide if should crawl from API or use tracks dataset 
    
daily_user_clickstream_etl()