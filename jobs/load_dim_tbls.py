from typing import TYPE_CHECKING, List
from pyspark.sql import SparkSession
from spotify_api_client.session import APISession
from faker import Faker
from faker_custom_providers import user_info

from pyspark import RDD
from pyspark.sql import DataFrame


def main(access_token: dict, genres: List[str]):
    # Create SparkSession with Hive support
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    
    # Request data from API - only when respective tables are empty
    tracks_empty = spark.table("dim_tracks").isEmpty()
    artists_empty = spark.table("dim_tracks").isEmpty()
    
    if tracks_empty or artists_empty:
        with APISession(access_token) as client_session:
            rdd_track_objs = sc.emptyRDD()

            for g in genres:
                rdd_tmp = sc.parallelize(
                    client_session.search_items(
                        q = "genres:" + g,
                        type = "track",
                        limit = 50,
                        offset = 0,
                        recursive = True
                    )["tracks"]
                )
                rdd_track_objs = rdd_track_objs.union(rdd_tmp)

        # Cache for later usage
        rdd_track_objs.cache()

        # Process and write data
        if tracks_empty:
            df_tracks = rdd_objs_to_df_tracks(rdd_track_objs)
            df_tracks.write.insertInto("dim_tracks")
        if artists_empty:
            df_artists = rdd_objs_to_df_artists(rdd_track_objs)
            df_artists.write.insertInto("dim_artists")

        # Unpersist previously cached data
        rdd_track_objs.unpersist()

    # Generate fake users and dates
    # Simple logic to avoid appending duplicated generated data
    # If some of the rows are deleted manually, this logic would not be able
    # to detect if rows are fully loaded
    if spark.table("dim_users").isEmpty():
        df_users = generate_df_users(no_users = 10000)
        df_users.write.insertInto("dim_users")
    if spark.table("dim_dates").isEmpty():
        df_dates = generate_df_dates("2018-01-01", "2028-01-01")
        df_dates.write.insertInto("dim_dates")


def rdd_objs_to_df_tracks(rdd_objs: RDD) -> DataFrame:
    """Convert response track objects into track DataFrame"""
    spark = SparkSession.getActiveSession()

    # Parse fields in each object
    rdd_tracks = rdd_objs \
        .map(
            lambda obj: {
                "track_id": obj["id"],
                "track_name": obj["name"],
                "track_duration_ms": obj["duration_ms"],
                "artist_ids": [a["id"] for a in obj["artists"]],
                "album_name": obj["album"]["name"],
                "album_type": obj["album"]["album_type"],
                "album_release_date": obj["album"]["release_date"]
            }
        )
    
    # Create DataFrame and rearrange columns
    df_tracks = spark \
        .createDataFrame(rdd_tracks) \
        .select(
            *("track_id", "track_name", "track_duration_ms"),
            *("artist_ids", "album_name", "album_type", "album_release_date")
        )
    
    return df_tracks


def rdd_objs_to_df_artists(rdd_objs: RDD) -> DataFrame:
    """Convert response track objects to artist DataFrame"""
    spark = SparkSession.getActiveSession()
    
    # Predefine some lambda functions for readability
    artist_record = lambda a: {"artist_id": a["id"], "artist_name": a["name"]}
    add = lambda l1, l2: l1 + l2

    # Parse artist info in track albums
    rdd_album_artists = rdd_objs.map(
        lambda obj: [artist_record(a) for a in obj["album"]["artists"]]
    )
    
    # Parse artist info in track artists
    rdd_track_artists = rdd_objs.map(
        lambda obj: [artist_record(a) for a in obj["artists"]]
    ) 

    # Reduce artist info lists and redistribute into rdd
    rdd_artists = spark.sparkContext.parallelize(
        rdd_album_artists.union(rdd_track_artists).reduce(add)
    )
    
    # Create DataFrame, rearrange columns
    # Remove duplicates because artists-tracks has M:M relationship 
    df_artists = spark \
        .createDataFrame(rdd_artists) \
        .select("artist_id", "artist_name") \
        .drop_duplicates(subset = ["artist_id"])
    
    return df_artists


def generate_df_users(no_users: int) -> DataFrame:
    """Generate user DataFrame consisting profiles of no_users users"""
    spark = SparkSession.getActiveSession()

    fake = Faker()
    fake.add_provider(user_info.Provider)

    df_users = spark \
        .createDataFrame(
            spark.sparkContext.parallelize(
                [fake.user_profile() for _ in range(no_users)]
            )
        ) \
        .select(
            *("user_id", "username"),
            *("sex", "address", "mail", "birthdate")
        ) \
        .drop_duplicates(subset = ["user_id"])

    return df_users


def generate_df_dates(start_date: str, end_date: str) -> DataFrame:
    """Populate calendar date from start_date to end_date"""
    spark = SparkSession.getActiveSession()

    # Reference
    # (https://3cloudsolutions.com/resources/generate-a-calendar-dimension-in-spark/)
    spark.sql(f"""
        SELECT EXPLODE(
            SEQUENCE(
                TO_DATE('{start_date}'), 
                TO_DATE('{end_date}'), 
                INTERVAL 1 day
            )
        ) AS full_date;
    """) \
        .createOrReplaceTempView("dates")
    df_dates = spark.sql(f"""
        SELECT 
            (
                (YEAR(full_date) * 10000) 
                + MONTH(full_date) * 100 
                + DAY(full_date)
            ) AS date_dim_id,
            full_date, 
            YEAR(full_date) AS year,
            MONTH(full_date) AS month,
            DAY(full_date) AS day,
            WEEKOFYEAR(full_date) AS week_of_year,
            DAYOFWEEK(full_date) AS day_of_week
        FROM dates;
    """)

    return df_dates


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument(
        *("-t", "--token"), 
        help = "Spotify client access token, as json string",
        dest = "access_token",
        required = True
    )
    parser.add_argument(
        *("-g", "--genres"), 
        nargs = "*",
        help = "Track genres to request to Spotify API",
        dest = "genres",
        required = True
    )
    args = parser.parse_args()

    access_token = json.loads(args.access_token)
    genres = args.genres

    main(access_token, genres)