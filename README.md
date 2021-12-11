# Project Overview
Sparkify is a startup wants to analyze the songs they have been collecting and keep track of the users activity on their music Application. As my role (Data Engineer) I will help them building their Databases.

# Project Description

## Project Structure

```
.
├── assets
│   └── images
│       ├── staging_tables.svg
│       └── tables.svg
├── create_tables.py
├── data
│   └── data
│       ├── log_data (user log folder)
│       │   ├── 2018-11-01-events.json
│       │   ├── .....
│       └── song_data (folder containing songs and artists data)
│           ├── TRAAAAW128F429D538.json
│           ├── .....
├── dwh.cfg (config contains settings for redshift cluster)
├── etl.py
├── README.md
├── requirements.txt
├── sql_queries.py
├── staging_events_jsonpath.json
└── tree.txt

7 directories, 112 files

```

## Tables

The design of the database schema will be based on [***Star Schema***](https://en.wikipedia.org/wiki/Star_schema), So we will have, in this project, **one** fact table and **four** dimension tables. Also as we are going to use [***Redshift***](https://aws.amazon.com/redshift/) we will have two staging tables ***staging_events*** and ***staging_songs***.

### Tables

![Tables Schema](/assets/images/tables.svg)

### Staging Tables

![Tables Schema](/assets/images/staging_tables.svg)

## Getting Started

### Installing Dependencies

#### Python
Follow instructions to install the latest version of python for your platform in the [Python Docs](https://docs.python.org/3/using/unix.html#getting-and-installing-the-latest-version-of-python)

#### Virtual Environment
It's recommend to work within a virtual environment whenever using Python for projects. This keeps your dependencies for each project separate and organized. Instructions for setting up a virtual environment for your platform can be found in the [Python Docs](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/)

#### PIP Dependencies
Once you have your virtual environment setup and running. Make sure you are in the right folder then install dependencies:
```
pip install -r requirements.txt
```
This will install all of the required packages within the `requirements.txt` file.

### Running Application

1. First you have to create an AWS account, please refer to this [link](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).

2. You have to create [Redshift Cluster](https://aws.amazon.com/redshift/) .

3. Create an IAM Role, please refer to this [link](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user.html).
    - In _Choose a use case_ section select **Redshift**.
    - In _Select your case_ section select **Redshift Customizable**.
    - Click Next
    - In _Attach permissions policies_ search for **AmazonS3ReadOnlyAccess** and select it.
    - Click Next
    - Review and Create.

3. Fill in `dwh.cfg` fields.

4. Create S3 Buckets, please refer to this [link](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)

5. Upload `log_data` and `song_data` in _data_ folder, each in a separate bucket.

6. Upload `staging_events_jsonpath.json` in a separate bucket also.

7. Run `create_tables.py`.
    - `python create_tables.py`

8. Run `etl.py`.
    - `python etl.py`

### Finally 

When you finish ***delete*** Redshift Cluster and buckets you have created to avoid charging.

## Tables In Details

### Fact Tables

##### Songplays

Create Query

```
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
```

Insert Query

```
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
```

### Dimension Tables

##### 1- Users

Create Query
```
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR)
diststyle all;
```

Insert Query

```
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
```

##### 2- Songs

Create Query

```
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY DISTKEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER SORTKEY,
    duration FLOAT);
```

Insert Query

```
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
```

##### 3- Artists

Create Query

```
CREATE TABLE IF NOT EXISTS  artists (
    artist_id VARCHAR PRIMARY KEY DISTKEY,
    name VARCHAR,
    location VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR);
```

Insert Query

```
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
```

##### 4- Time

Create Query

```
CREATE TABLE IF NOT EXISTS  time (
    id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL SORTKEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL);
```

Insert Query

```
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
```


### Staging Tables

##### Staging Events

Create Query

```
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
```

Insert Query

```
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
json {}
region {};
""").format(LOG_DATA, ARN, LOG_JSONPATH, REGION)
```

##### Staging Songs

Create Query

```
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
```

Insert Query

```
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
json 'auto'
region {}
""").format(SONG_DATA, ARN, REGION)
```
