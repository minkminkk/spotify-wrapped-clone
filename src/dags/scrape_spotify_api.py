from typing import List, Any
import pendulum
import logging

from airflow.decorators import dag, task, teardown, task_group
from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.providers.mongo.hooks.mongo import MongoHook  
from pymongo.errors import BulkWriteError      

@dag(
    dag_id = "scrape_spotify_api",
    schedule = None,
    start_date = pendulum.datetime(2018, 1, 1, tz = "UTC"),
    tags = ["scraping", "spotify", "api"],
    default_args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "show_return_value_in_logs": False, 
            # most tasks have huge outputs -> avoid log explosion
        "retries": 0    # dev only
    }
)
def scrape_spotify_api():
    @task
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


    """Request data from Spotify WebAPI"""
    @task_group(
        group_id = "request_data",
        prefix_group_id = False,
        default_args = {
            "retries": 3,
            "retry_delay": 5
        }
    )
    def request_data(access_token: dict[str, Any]):
        from utils.spotify_api_client.session import APISession

        @task
        def get_genres(access_token: dict):
            with APISession(access_token) as session:
                logging.info("Requesting genre data")
                r_json = session.get_genres()
                
                genres = r_json["genres"]
                logging.info(f"SUMMARY: Retrieved {len(genres)} genres")

            return genres

        @task
        def search_tracks(access_token: dict[str, Any], genre: List[str]):

            with APISession(access_token) as session:
                fields = ("id", "name", "duration_ms", "artists", "album")

                tracks = [
                    {f: item[f] for f in fields} \
                        for item in session.search_items(
                            q = "genres:" + genre,
                            type = "track",
                            limit = 50,
                            offset = 0,
                            recursive = True,
                            max_pages = 3
                        )["tracks"]
                ]
                
                logging.info(f"SUMMARY: Retrieved {len(tracks)} tracks")
                return tracks
            
        genres = get_genres(access_token)
        track_obj_list_maparg = search_tracks \
            .partial(access_token = access_token) \
            .expand(genre = ["alternative"])
        
        return track_obj_list_maparg
        

    """Process gathered data"""
    @task_group(
        group_id = "process_data", 
        prefix_group_id = False
    )
    def process_data():
        from functools import reduce

        @task
        def parse_album_info(task_instance: TaskInstance):
            objects = reduce(
                lambda l1, l2: l1 + l2,
                task_instance.xcom_pull(task_ids = "search_tracks")
            )

            # Get album info + artists field to parse album artists
            albums = [
                {
                    "_id": obj["album"]["id"], 
                    "name": obj["album"]["name"],
                    "album_type": obj["album"]["album_type"],
                    "artists": obj["album"]["artists"],
                    "artist_ids": [a["id"] for a in obj["album"]["artists"]],
                    "release_date": obj["album"]["release_date"] 
                } for obj in objects
            ]
            
            # Get album artists
            album_artists = reduce(
                lambda l1, l2: l1 + l2,
                [
                    [
                        {"_id": a["id"], "name": a["name"]} \
                            for a in al["artists"]
                    ] for al in albums
                ]
            )

            # Delete artists field when done parsing album artists
            [al.pop("artists") for al in albums]
            
            logging.info(f"SUMMARY: Retrieved {len(albums)} "
                + "different albums")
            
            task_instance.xcom_push("albums", albums)
            task_instance.xcom_push("album_artists", album_artists)


        @task
        def parse_artist_info(task_instance: TaskInstance):
            """Get artists in tracks and its albums. Deduplicate"""

            # Take artist list from each track and combine
            objects = reduce(
                lambda l1, l2: l1 + l2,
                task_instance.xcom_pull(task_ids = "search_tracks")
            )
            album_artists = task_instance.xcom_pull(
                task_ids = "parse_album_info",
                key = "album_artists"
            )

            track_artists = map(
                lambda obj: [
                    {
                        "_id": a["id"], 
                        "name": a["name"],
                    } for a in obj["artists"]
                ],
                objects
            )
            track_artists = reduce(
                lambda a_list1, a_list2: a_list1 + a_list2, 
                track_artists
            )
            
            # Extract fields from each artists. Deduplicate
            visited_ids = set()
            artists = []
            for a in track_artists + album_artists:
                if a["_id"] not in visited_ids:
                    artists.append(a)
                    visited_ids.add(a["_id"])
                
            logging.info(f"SUMMARY: Retrieved {len(artists)} "
                + "different artists from tracks and its albums")
            return artists

        @task
        def parse_track_info(task_instance: TaskInstance):
            objects = reduce(
                lambda l1, l2: l1 + l2,
                task_instance.xcom_pull(task_ids = "search_tracks")
            )

            track_info = list(
                map(
                    lambda obj: {
                        "_id": obj["id"],
                        "name": obj["name"],
                        "album_id": obj["album"]["id"],
                        "artist_ids": [a["id"] for a in obj["artists"]],
                        "duration_ms": obj["duration_ms"]
                    },
                    objects
                )
            )
            
            logging.info(f"SUMMARY: Processed {len(track_info)} tracks")
            return track_info
        
        albums = parse_album_info()
        # album_artists = parse_artists_in_albums()
        artists = parse_artist_info()
        tracks = parse_track_info()

        albums >> artists

        return [albums, artists, tracks]
    

    @task
    def generate_user_profiles(no_users: int):
        from faker import Faker
        from utils.faker_custom_providers import user_info

        fake = Faker()
        fake.add_provider(user_info.Provider)

        users = [fake.user_profile() for _ in range(no_users)]
        print(users[:5])
        return users


    # Get Mongo client and DB
    mongo_hook = MongoHook("mongo_default")
    client = mongo_hook.get_conn()
    db = client["spotifydb"]


    @task_group(group_id = "insert_to_mongo", prefix_group_id = False)
    def insert_to_mongo():
        # MongoDB do not have key duplicate support, therefore using
        # unordered batch insert (which still executes in case exception
        # thrown) and handle exception
        # NOTE: This approach is not ACID-safe therefore data could be
        # partially inserted in case of other exceptions
        # https://pymongo.readthedocs.io/en/stable/examples/bulk.html
        from functools import reduce

        @task
        def insert_albums(task_instance: TaskInstance):
            results = task_instance.xcom_pull(
                task_ids = "parse_album_info",
                key = "albums"
            )
            try:
                db["albums"].insert_many(results, ordered = False)
            except BulkWriteError as e:
                pass    # TODO: Move away from try except

        @task
        def insert_artists(task_instance: TaskInstance):
            results = task_instance.xcom_pull(task_ids = "parse_artist_info")
            try:
                db["artists"].insert_many(results, ordered = False)
            except BulkWriteError as e:
                pass    # TODO: Move away from try except

        @task
        def insert_tracks(task_instance: TaskInstance):
            results = task_instance.xcom_pull(task_ids = "parse_track_info")
            try:
                db["tracks"].insert_many(results, ordered = False)
            except BulkWriteError as e:
                pass    # TODO: Move away from try except

        @task
        def insert_users(task_instance: TaskInstance):
            results = task_instance.xcom_pull(task_ids = "generate_user_profiles")
            try:
                db["users"].insert_many(results, ordered = False)
            except BulkWriteError as e:
                pass    # TODO: Move away from try except

        return [
            insert_albums(), insert_artists(), insert_tracks(), insert_users()
        ]
    

    # Task run successfully but XCom of DAG run not deleted 
    # TODO: Make this work
    @provide_session
    @teardown
    def clean_xcom(session, run_id: str):
        session.query(XCom).filter(XCom.dag_run_id == run_id).delete()
        session.commit()

    
    # Execute tasks
    access_token = get_access_token()
    initial_data = request_data(access_token)
    processed_data = process_data()
    users = generate_user_profiles(1000)
    insert_mongo = insert_to_mongo()
    cleanup = clean_xcom()
    
    # Set dependencies between task groups
    access_token >> initial_data >> processed_data
    processed_data[0] >> insert_mongo[0]
    processed_data[1] >> insert_mongo[1]
    processed_data[2] >> insert_mongo[2]
    users >> insert_mongo[3]
    insert_mongo >> cleanup


scrape_spotify_api()