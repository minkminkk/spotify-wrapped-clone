from pyspark.sql import SparkSession, DataFrame


def main():
    # Create SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("Prepare Hive tables") \
        .description("Create Hive tables. Load dates dimension data.") \
        .enableHiveSupport() \
        .getOrCreate()

    # DDL queries for creating Hive tables
    q_tracks = """
        CREATE TABLE IF NOT EXISTS dim_tracks (
            track_dim_id        BIGINT,
            track_id            CHAR(22),
            track_name          VARCHAR(120),
            track_duration_ms   INTEGER,
            artist_ids          ARRAY<BIGINT>,
            album_name          VARCHAR(120),
            album_type          VARCHAR(11),
            album_release_date  DATE
        ) USING parquet;
    """
    q_artists = """
        CREATE TABLE IF NOT EXISTS dim_artists (
            artist_dim_id   BIGINT,
            artist_id       CHAR(22),
            artist_name     VARCHAR(120)
        ) USING parquet;
    """
    q_users = """
        CREATE TABLE IF NOT EXISTS dim_users (
            user_dim_id     BIGINT,
            user_id         CHAR(22),
            username        VARCHAR(64),
            name            VARCHAR(120),
            sex             CHAR(1),
            address         VARCHAR(250),
            mail            VARCHAR(80),
            birthdate       DATE
        ) USING parquet;
    """
    q_dates = """
        CREATE TABLE IF NOT EXISTS dim_dates (
            date_dim_id     INTEGER,
            full_date       DATE,
            year            SMALLINT,
            month           TINYINT,
            day             TINYINT,
            week_of_year    TINYINT,
            day_of_week     TINYINT
        ) USING parquet;
    """
    q_events = """
        CREATE TABLE IF NOT EXISTS fct_user_events (
            event_id        CHAR(64),
            event_ts        TIMESTAMP,
            event_name      VARCHAR(4),
            user_dim_id     BIGINT,
            ipv4            VARCHAR(15),
            user_agent      VARCHAR(250),
            track_dim_id    BIGINT,
            date_dim_id     INT
        ) USING parquet PARTITIONED BY (date_dim_id);
    """

    for q in (q_tracks, q_artists, q_users, q_dates, q_events):
        spark.sql(q)

    # Populate dates dimension table
    cur_rows = spark.table("dim_dates").count()
    
    # Simple logic to avoid overwriting
    # If some of the rows are deleted manually, this logic would not be able
    # to detect if rows are fully loaded
    if cur_rows == 0:
        df_dates = populate_date_df("2018-01-01", "2028-01-01")
        df_dates.write.insertInto("dim_dates")


def populate_date_df(start_date: str, end_date: str) -> DataFrame:
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
    main()