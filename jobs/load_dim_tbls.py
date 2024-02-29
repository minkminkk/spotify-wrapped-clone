from typing import List
from pyspark.sql import SparkSession
from session import APISession


def main(access_token: dict, genres: List[str]):
    # Create SparkSession with Hive support
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sc = spark.sparkContext()
    
    with APISession(access_token) as client_session:
        rdd_track_objs = sc.emptyRDD()

        for g in genres:
            rdd_tmp = sc.parallelize(
                client_session.search_tracks(
                    q = "genres:" + g,
                    type = "track",
                    limit = 50,
                    offset = 0,
                    recursive = False
                )
            )
            rdd_track_objs = rdd_track_objs.union(rdd_tmp)

        rdd_tracks = rdd_track_objs.map(
            lambda obj: {
                "_id": obj["id"],
                "name": obj["name"],
                "album_id": obj["album"]["id"],
                "artist_ids": [a["id"] for a in obj["artists"]],
                "duration_ms": obj["duration_ms"]
            }
        )
        df_tracks = spark.createDataFrame(rdd_tracks)
        df_tracks.show(20)
        df_tracks.printSchema()
    


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
    args = parser.parse_args()

    access_token = json.loads(args.access_token)
    main(access_token)