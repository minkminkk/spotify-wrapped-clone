from typing import List
from pyspark.sql import SparkSession, DataFrame, Row, functions as F
from pyspark.sql.types import *
from packages.faker_custom_providers import user_clickstream
from faker import Faker
from datetime import datetime, timedelta


def main(data_date: datetime, local: bool):
    # Create SparkSession with Hive support
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Get random 10% of all users as today's active users
    map_user_track_ids = spark.table("dwh.dim_users") \
        .select("user_id", "track_ids") \
        .sample(fraction = 0.1) \
        .collect()
    
    # Generate clickstream data from active users
    df = generate_clickstream_to_df(
        start_dt = data_date, 
        end_dt = data_date + timedelta(days = 1), 
        map_user_track_ids = map_user_track_ids, 
        max_events_per_user = 100
    )
    df = df.withColumn("event_year", F.year("event_ts")) \
        .withColumn("event_month", F.month("event_ts")) \
        .withColumn("event_day", F.day("event_ts"))
    
    # Write events as parquet
    df.show()
    out_path = "./data/user_events.parquet" if local \
        else "hdfs://namenode:8020/data_lake/user_events.parquet"
    df.write.save(
        path = out_path,
        format = "parquet",
        mode = "append",
        partitionBy = ["event_year", "event_month", "event_day"]
    )


def generate_clickstream_to_df(
    start_dt: datetime, 
    end_dt: datetime,
    map_user_track_ids: List[Row] | dict,
    max_events_per_user: int
) -> DataFrame:
    """Generate clickstream data within time period"""
    spark = SparkSession.getActiveSession()

    # Create Faker object
    fake = Faker()
    fake.add_provider(user_clickstream.Provider)

    # Generate data
    rows = []
    for user_track_id in map_user_track_ids:
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

    df = spark.createDataFrame(
        rows,
        schema = StructType(
            [
                StructField("event_id", StringType()),
                StructField("event_ts", TimestampType()),
                StructField("event_name", StringType()),
                StructField("ipv4", StringType()),
                StructField("user_id", StringType()),
                StructField("user_agent", StringType()),
                StructField("track_id", StringType())
            ]
        )
    )

    return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("data_date", help = "Data date (ISO format)")
    parser.add_argument(
        "-l", "--local",
        action = "store_true",
        help = "Run as local"
    )
    args = parser.parse_args()

    args.data_date = datetime.fromisoformat(args.data_date)

    main(args.data_date, args.local)