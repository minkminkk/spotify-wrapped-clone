from pyspark.sql import SparkSession, DataFrame, Window, functions as F
from packages.faker_custom_providers import user_clickstream
from faker import Faker
from datetime import datetime


def main(start_dt: datetime | str, end_dt: datetime | str):
    # Create SparkSession with Hive support
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    user_ids = spark.table("dwh.dim_users") \
        .sample(fraction = 0.1) \
        .select("user_id", "track_ids") \
        .collect()
    
    # Generate data
    df = generate_clickstream_to_df(start_dt, end_dt, user_ids, 10)
    df = df.withColumn("event_year", F.year("event_ts")) \
        .withColumn("event_month", F.month("event_ts")) \
        .withColumn("event_day", F.day("event_ts"))
    
    # TODO: df.write.parquet

    # TODO: split data transform and loading into another task
    df = df.drop("event_year", "event_month", "event_day")
    df_agg = sum_playtime_from_clickstream(df)

    # Map dimensions to its respective ID
    #TODO: Map dim_id to users, tracks
    df_agg = map_dates_with_dim_id(df_agg)
    
    df_agg.show()


def generate_clickstream_to_df(
    start_dt: datetime | str, 
    end_dt: datetime | str,
    user_ids: int,
    max_events_per_user: int
) -> DataFrame:
    """Generate clickstream data within time period"""
    spark = SparkSession.getActiveSession()

    # Create Faker object
    fake = Faker()
    fake.add_provider(user_clickstream.Provider)
    
    # Generate data
    rows = []
    for user in user_ids:
        rows.extend(
            [
                event for event in fake.events_from_one_user(
                    start_dt = start_dt,
                    end_dt = end_dt,
                    user_id = user["user_id"],
                    user_tracklist = user["track_ids"],
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("start_dt", help = "Start date (YYYY-MM-DD)")
    parser.add_argument("end_dt", help = "End date (YYYY-MM-DD)")
    args = parser.parse_args()

    if isinstance(args.start_dt, str):
        args.start_dt = datetime.fromisoformat(args.start_dt)
        args.end_dt = datetime.fromisoformat(args.end_dt)

    main(args.start_dt, args.end_dt)