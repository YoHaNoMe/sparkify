import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
    events_id INTEGER IDENTITY(0, 1) NOT NULL,
    artist VARCHAR DISTKEY,
    auth VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR,
    item_in_session INTEGER,
    length VARCHAR,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    session_id INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT SORTKEY,
    user_agent  VARCHAR,
    user_id INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    id INTEGER IDENTITY(0, 1) NOT NULL,
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name VARCHAR DISTKEY,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT SORTKEY,
    year INTEGER);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) NOT NULL,
    start_time  BIGINT NOT NULL,
    user_id     VARCHAR NOT NULL,
    level       VARCHAR NOT NULL,
    song_id     VARCHAR  NOT NULL,
    artist_id   VARCHAR NOT NULL DISTKEY,
    session_id  VARCHAR NOT NULL,
    location    VARCHAR,
    user_agent  VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY DISTKEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER SORTKEY,
    duration FLOAT);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS  artists (
    artist_id VARCHAR PRIMARY KEY DISTKEY,
    name VARCHAR,
    location VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS  time (
    id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL SORTKEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
json {}
region 'us-east-1';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
json 'auto'
region 'us-east-1'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
    SELECT
    se.ts AS start_time,
    se.user_id AS user_id,
    se.level AS level,
    ss.artist_id AS artist_id,
    ss.song_id AS song_id,
    se.session_id AS session_id,
    se.location AS location,
    se.user_agent AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss ON (se.artist = ss.artist_name)
    WHERE se.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO users(
    user_id,
    first_name,
    last_name,
    gender,
    level)
    SELECT
    se.user_id AS user_id,
    se.first_name AS first_name,
    se.last_name AS last_name,
    se.gender AS gender,
    se.level AS level
    FROM staging_events AS se;
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration)
    SELECT
    ss.song_id AS song_id,
    ss.title AS title,
    ss.artist_id AS artist_id,
    ss.year AS year,
    ss.duration AS duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude)
    SELECT
    ss.artist_id AS artist_id,
    ss.artist_name AS name,
    ss.artist_location AS location,
    ss.artist_latitude AS latitude,
    ss.artist_longitude AS longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
    SELECT
    dateadd(
    s, convert(bigint, se.ts) / 1000, convert(datetime, '1-1-1970 00:00:00')
    ) AS start_time,
    EXTRACT(hour from start_time) AS hour,
    EXTRACT(day from start_time) AS day,
    EXTRACT(week from start_time) AS week,
    EXTRACT(month from start_time) AS month,
    EXTRACT(year from start_time) AS year,
    date_part(dow, start_time) AS weekday
    FROM staging_events AS se;
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [
   staging_events_copy,
   staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
