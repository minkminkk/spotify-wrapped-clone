#TODO: 
# - Use MongoClient and utils.faker_custom_provider to ingest data into MongoDB
# - Use Spotify tracks dataset to get available track_ids 
# and use it as sample for faking data
# (https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset)
#       - Download dataset and use Airflow to save track_id column as another file
#       - When generating data, read data from that file

from typing import List
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.decorators.setup_teardown import setup_task, teardown_task
from airflow.models.variable import Variable


@dag(
    dag_id = "daily_user_clickstream_etl",
    schedule = "@daily",
    start_date = pendulum.datetime(2018, 1, 1, tz = "UTC"),
    tags = ["etl"],
    default_args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0
    }
)
def daily_user_clickstream_etl():
    import logging


    @setup_task
    def get_access_token():
        from utils.spotify_api_client.auth \
            import ClientAuthenticator, ClientCredentialsStrategy
        
        # Get client_id and client secret for auth
        client_id = Variable.get("spotify_webapi_client_id", None)
        client_secret = Variable.get("spotify_webapi_client_secret", None)

        if not client_id or not client_secret:
            error_msg = "Variables spotify_webapi_client_id and " \
                + "spotify_webapi_client_secret not found."
            raise Exception(error_msg)

        # Generate auth object and get access token
        auth = ClientAuthenticator(client_id, client_secret)
        auth.set_strategy(ClientCredentialsStrategy())

        return auth.get_access_token()
    access_token = get_access_token()


    @task_group(
        group_id = "request_data",
        prefix_group_id = False,
        default_args = {
            "retries": 3,
            "retry_delay": 5
        }
    )
    def request_data(access_token):
        from utils.spotify_api_client.session import APISession

        @task
        def get_genres(access_token: dict) -> List[str]:
            with APISession(access_token) as session:
                logging.info("Requesting genre data")
                r_json = session.get_genres()
                
                genres = r_json["genres"]
                logging.info(f"SUMMARY: Retrieved {len(genres)} genres")

            return genres
        genres = get_genres(access_token)


        @task
        def search_tracks(access_token: dict, genres: List[str]) -> List[dict]:
            with APISession(access_token) as session:
                tracks = []

                for g in genres:
                    tracks.extend(
                        session.search_items(
                            q = "genres:" + g,
                            type = "track",
                            limit = 20,
                            offset = 0,
                            recursive = True,
                            max_pages = 1
                        )["tracks"]
                    )
                
                logging.info(f"SUMMARY: Retrieved {len(tracks)} tracks")
                return tracks
        tracks = search_tracks(access_token, genres)
    request_data(access_token)

    
daily_user_clickstream_etl()