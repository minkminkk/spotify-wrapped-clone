DROP DATABASE IF EXISTS dwh CASCADE;
CREATE DATABASE IF NOT EXISTS dwh;


CREATE TABLE IF NOT EXISTS dwh.dim_tracks (
    track_dim_id        BIGINT,
    track_name          STRING,
    artists             ARRAY<STRING>,
    album_name          STRING,
    track_genre         STRING,
    duration_ms         INTEGER
) USING parquet;


CREATE TABLE IF NOT EXISTS dwh.dim_users (
    user_dim_id     BIGINT,
    user_id         CHAR(22),
    username        STRING,
    name            STRING,
    sex             CHAR(1),
    address         STRING,
    email           STRING,
    birth_date      DATE
) USING parquet;


CREATE TABLE IF NOT EXISTS dwh.dim_dates (
    date_dim_id     INTEGER,
    full_date       DATE,
    year            SMALLINT,
    month           TINYINT,
    day             TINYINT,
    week_of_year    TINYINT,
    day_of_week     TINYINT
) USING parquet;


CREATE TABLE IF NOT EXISTS dwh.fct_user_events (
    event_id        CHAR(64),
    event_ts        TIMESTAMP,
    event_name      VARCHAR(4),
    user_dim_id     BIGINT,
    ipv4            VARCHAR(15),
    user_agent      VARCHAR(250),
    track_dim_id    BIGINT,
    date_dim_id     INT
) USING parquet PARTITIONED BY (date_dim_id);


CREATE TABLE IF NOT EXISTS dwh.fct_trackplays (
    start_date_dim_id   CHAR(64),
    user_dim_id         BIGINT,
    track_dim_id        BIGINT,
    play_duration_ms    INTEGER
) USING parquet PARTITIONED BY (start_date_dim_id);