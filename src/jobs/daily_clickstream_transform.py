from pyspark.sql import SparkSession, DataFrame, Window, functions as F
from datetime import datetime


def main(data_date: datetime, local: bool):
    # Create SparkSession with Hive support
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Get partition path
    partition_suffix = f"event_year={data_date.year}/" \
        + f"event_month={data_date.month}/" \
        + f"event_day={data_date.day}"
    partition_path = "./data/user_events.parquet/" if local \
        else "hdfs://namenode:8020/data_lake/user_events.parquet/"
    partition_path += partition_suffix

    # Read partition
    df = spark.read.parquet(partition_path) \
        .drop("event_year", "event_month", "event_day")

    # Aggregate track playtime from clickstream
    df_agg = sum_playtime_from_clickstream(df)

    # Map dimensions to its respective ID
    df_agg = map_dates_with_dim_id(df_agg)
    df_agg = map_users_with_dim_id(df_agg)
    df_agg = map_tracks_with_dim_id(df_agg)
    df_agg = df_agg.select(
        "user_dim_id",
        "track_dim_id",
        "play_duration_ms",
        "play_start_date_dim_id"
    )   # insertInto write DF based on position therefore need rearrange cols
    
    # Append into fact table
    df_agg.show()
    # df_agg.write.insertInto("dwh.fct_trackplays", overwrite = True)


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
        F.date_format(df_agg["play_start_ts"], "yyyyMMdd").cast("integer")
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
        how = "inner"
    ).drop("track_id")
    
    return df_agg


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("data_date", help = "Date of data (ISO format)")
    parser.add_argument(
        "-l", "--local",
        action = "store_true",
        help = "Run as local"
    )
    args = parser.parse_args()

    args.data_date = datetime.fromisoformat(args.data_date)

    main(args.data_date, args.local)