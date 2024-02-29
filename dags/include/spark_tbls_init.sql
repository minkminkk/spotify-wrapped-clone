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


CREATE TABLE IF NOT EXISTS dim_artists (
    artist_dim_id   BIGINT,
    artist_id       CHAR(22),
    artist_name     VARCHAR(120)
) USING parquet;


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


CREATE TABLE IF NOT EXISTS dim_dates (
    date_dim_id     INTEGER,
    full_date       DATE,
    year            SMALLINT,
    month           TINYINT,
    day             TINYINT,
    week_of_year    TINYINT,
    day_of_week     TINYINT
) USING parquet;


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