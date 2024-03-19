from pyspark.sql import SparkSession


def main():
    # Create SparkSession with Hive support
    spark = SparkSession.builder \
        .appName("Prepare Hive tables") \
        .enableHiveSupport() \
        .getOrCreate()

    # DDL queries for creating Hive tables
    q_tracks = """
        CREATE TABLE IF NOT EXISTS dim_tracks (
            track_dim_id        BIGINT,
            artists             ARRAY<VARCHAR>,
            album_name          VARCHAR,
            track_name          VARCHAR,
            popularity          TINYINT,
            duration_ms         INTEGER,
            explicit            BOOLEAN,
            danceability        FLOAT,
            energy              FLOAT,
            key                 TINYINT,
            loudness            FLOAT,
            mode                TINYINT,
            speechiness         FLOAT,
            acousticness        FLOAT,
            instrumentalness    FLOAT,
            liveness            FLOAT,
            valence             FLOAT,
            tempo               FLOAT,
            time_signature      TINYINT,
            track_genre         VARCHAR
        ) USING parquet;
    """
    q_users = """
        CREATE TABLE IF NOT EXISTS dim_users (
            user_dim_id     BIGINT,
            username        VARCHAR,
            name            VARCHAR,
            sex             CHAR(1),
            address         VARCHAR,
            email           VARCHAR,
            birth_date      DATE
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
        CREATE TABLE IF NOT EXISTS fct_trackplays (
            start_date_dim_id   CHAR(64),
            user_dim_id         BIGINT,
            track_dim_id        BIGINT,
            artist_dim_id       BIGINT,
            play_duration_ms    INTEGER
        ) USING parquet PARTITIONED BY (start_date_dim_id);
    """

    for q in (q_tracks, q_users, q_dates, q_events):
        spark.sql(q)


if __name__ == "__main__":
    main()