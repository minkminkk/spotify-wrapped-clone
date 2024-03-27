from pyspark.sql import SparkSession, DataFrame, Window, functions as F
from packages.faker_custom_providers import user_clickstream
from faker import Faker
from datetime import datetime


def main(start_dt: datetime | str, end_dt: datetime | str):
    # Create SparkSession with Hive support
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Get random 10% of all users as today's active users
    user_ids = spark.table("dwh.dim_users") \
        .sample(fraction = 0.1) \
        .select("user_id", "track_ids") \
        .collect()
    
    # Generate clickstream data from active users
    df = generate_clickstream_to_df(start_dt, end_dt, user_ids, 10)
    df.show()
    df = df.withColumn("event_year", F.year("event_ts")) \
        .withColumn("event_month", F.month("event_ts")) \
        .withColumn("event_day", F.day("event_ts"))
    
    # Write events as parquet
    # df.write \
    #     .parquet("/data_lake/user_events.parquet", mode = "append") \
    #     .partitionBy("event_year", "event_month", "event_day")

    # TODO: split data transform and loading into another task
    # df = df.drop("event_year", "event_month", "event_day")
    # df_agg = sum_playtime_from_clickstream(df)

    # Map dimensions to its respective ID
    #TODO: Map dim_id to users, tracks
    # df_agg = map_dates_with_dim_id(df_agg)
    # df_agg = map_users_with_dim_id(df_agg)
    # df_agg = map_tracks_with_dim_id(df_agg)
    # df_agg = df_agg.select(
    #     "play_start_date_dim_id",
    #     "user_dim_id",
    #     "track_dim_id",
    #     "play_duration_ms"
    # )
    
    # df_agg.show()

    # Append into fact table
    # df_agg.write.insertInto("dwh.fct_trackplays")


def generate_clickstream_to_df(
    start_dt: datetime | str, 
    end_dt: datetime | str,
    df_user_track_ids: DataFrame,
    max_events_per_user: int
) -> DataFrame:
    """Generate clickstream data within time period"""
    spark = SparkSession.getActiveSession()

    # Create Faker object
    fake = Faker()
    fake.add_provider(user_clickstream.Provider)

    # Generate data
    rows = []
    for user_track_id in df_user_track_ids:
        rows.extend(
            [
                event for event in fake.events_from_one_user(
                    start_dt = start_dt,
                    end_dt = end_dt,
                    user_id = user_track_id["user_id"],
                    user_tracklist = user_track_id["track_ids"],
                    max_events = max_events_per_user
                )
            ]
        )
    df = spark.createDataFrame(rows)

    return df


def sum_playtime_from_clickstream(df: DataFrame) -> DataFrame:
    """Sum track playtimes from clickstream events"""
    # (user_id, track_id, event_ts) -> trackplay
    w = Window.partitionBy("user_id", "track_id").orderBy("event_ts")

    # Find "play" events
    df = df.withColumn("prev_event_ts", F.lag("event_ts").over(w))
    
    # Find trackplay duration (at "stop" events, = 0 for "play" events)
    # and starting timestamps
    df = df.withColumn(
        "play_duration_ms", 
        F.when(
            df["event_name"] == "stop",
            F.unix_millis(df["event_ts"]) - F.unix_millis(df["prev_event_ts"])
        ).otherwise(0)
    ).withColumn(
        "play_start_ts",
        F.when(
            df["event_name"] == "play", df["event_ts"]
        ).otherwise(df["prev_event_ts"])
    ).drop("prev_event_ts")

    # Sum trackplay duration by trackplays
    df_agg = df.groupBy("user_id", "track_id", "play_start_ts") \
        .agg(F.sum("play_duration_ms").alias("play_duration_ms"))
    
    return df_agg


def map_dates_with_dim_id(df_agg: DataFrame) -> DataFrame:
    """Map dates to date dimension ID"""

    return df_agg.withColumn(
        "play_start_date_dim_id", 
        F.date_format(df_agg["play_start_ts"], "yyyyMMdd")
    ).drop("play_start_ts")


def map_users_with_dim_id(df_agg: DataFrame) -> DataFrame:
    """Map users to user dimension ID"""
    spark = SparkSession.getActiveSession()
    df_users = spark.table("dwh.dim_users") \
        .select("user_dim_id", "user_id")

    df_agg = df_agg.join(
        df_users, 
        on = df_agg["user_id"] == df_users["user_id"],
        how = "left"
    ).drop("user_id")
    
    return df_agg


def map_tracks_with_dim_id(df_agg: DataFrame) -> DataFrame:
    """Map tracks to track dimension ID"""
    spark = SparkSession.getActiveSession()
    df_tracks = spark.table("dwh.dim_tracks") \
        .select("track_dim_id", "track_id")

    df_agg = df_agg.join(
        df_tracks, 
        on = df_agg["track_id"] == df_tracks["track_id"],
        how = "left"
    ).drop("track_id")
    
    return df_agg


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("start_dt", help = "Start date (YYYY-MM-DD)")
    parser.add_argument("end_dt", help = "End date (YYYY-MM-DD)")
    parser.add_argument(
        "-l", "--local",
        action = "store_true",
        help = "Run as local"
    )
    args = parser.parse_args()

    for dt in (args.start_dt, args.end_dt):
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)

    main(args.start_dt, args.end_dt)