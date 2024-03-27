from faker import Faker
from packages.faker_custom_providers import user_info

from pyspark.sql import SparkSession, DataFrame, Window, functions as F
from pyspark.sql.types import LongType


def main(local: bool):
    # Create SparkSession with Hive support
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Read source data into DataFrame and rename columns
    data_path = "./data/spotify-tracks.parquet" if local \
        else "hdfs://namenode:8020/data_lake/spotify-tracks.parquet"
    df = spark.read.parquet(data_path)
    df = df.withColumnRenamed(df.columns[0], "id")

    # Process into dimension tables
    df_tracks = get_df_tracks(df)
    df_users = generate_df_users(no_users = 10000, df_tracks = df_tracks)
    df_dates = generate_df_dates("2018-01-01", "2028-01-01")

    # Write into Hive dimension tables
    df_tracks.write.insertInto("dwh.dim_tracks", overwrite = True)
    df_users.write.insertInto("dwh.dim_users", overwrite = True)
    df_dates.write.insertInto("dwh.dim_dates", overwrite = True)


def get_df_tracks(df: DataFrame) -> DataFrame:
    """Drop source track_id for consistency with other DataFrames.
    Get dimension ID based on row ID. Convert artist strings into arrays.
    """
    cols = (
        "track_dim_id",
        "track_id",
        "track_name",
        "artists",
        "album_name",
        "track_genre",
        "duration_ms"
    )

    return df \
        .withColumn("id", df["id"] + 1) \
        .withColumnRenamed("id", "track_dim_id") \
        .withColumn("artists", F.split("artists", ";")) \
        .select(*cols)


def generate_df_users(no_users: int, df_tracks: DataFrame) -> DataFrame:
    """Generate user DataFrame consisting profiles of no_users users"""
    spark = SparkSession.getActiveSession()

    fake = Faker()
    fake.add_provider(user_info.Provider)

    # Generate dim_id serial column using Window and F.row_number()
    w = Window.orderBy("name")
    df_users = spark \
        .createDataFrame(
            spark.sparkContext.parallelize(
                [fake.user_profile() for _ in range(no_users)]
            )
        ) \
        .withColumn("user_dim_id", F.row_number().over(w).cast(LongType())) \
        .withColumn(
            "track_ids", 
            F.lit(
                df_tracks \
                    .sample(fraction = 0.0005) \
                    .select("track_id") \
                    .rdd.flatMap(lambda x: x) \
                    .collect()
            )
        ) \
        .withColumnsRenamed(
            {
                "mail": "email",
                "birthdate": "birth_date"
            }
        ) \
        .select(
            *("user_dim_id", "user_id", "username", "name"),
            *("sex", "address", "email", "birth_date", "track_ids")
        )
        
    return df_users


def generate_df_dates(start_date: str, end_date: str) -> DataFrame:
    """Populate calendar date from start_date to end_date (inclusive)"""
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

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l", "--local",
        action = "store_true",
        help = "Run as local" 
    )
    args = parser.parse_args()
    
    main(args.local)