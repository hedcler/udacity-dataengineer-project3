import configparser

# DROP TABLES
staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"

songplay_table_drop = "drop table if exists songplays cascade"
user_table_drop = "drop table if exists users cascade"
song_table_drop = "drop table if exists songs cascade"
artist_table_drop = "drop table if exists artists cascade"
time_table_drop = "drop table if exists time cascade"

# CREATE TABLES
staging_events_table_create= ("""
  CREATE TABLE IF NOT EXISTS staging_events (
    artist          varchar,
    auth            varchar,
    firstName       varchar,
    gender          varchar,
    itemInSession   int,
    lastName        varchar,
    length          numeric,
    level           varchar,
    location        varchar,
    method          varchar,
    page            varchar,
    registration    varchar,
    sessionId       varchar,
    song            varchar,
    status          varchar,
    ts              timestamp,
    userAgent       varchar,
    userId          varchar
  );
""")

staging_songs_table_create = ("""
  CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs         varchar,
    artist_id         varchar,
    artist_latitude   numeric,
    artist_longitude  numeric,
    artist_location   varchar,
    artist_name       varchar,
    song_id           varchar,
    title             varchar,
    duration          float,
    year              smallint
  );
""")

songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays (
    songplay_id  bigint identity(0,1) primary key sortkey,
    start_time   timestamp not null,
    user_id      varchar not null distkey,
    level        varchar not null,
    song_id      varchar not null,
    artist_id    varchar not null,
    session_id   varchar not null,
    location     varchar,
    user_agent   varchar
  );
""")

user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users (
    user_id      varchar primary key not null sortkey distkey,
    first_name   varchar not null,
    last_name    varchar not null,
    gender       varchar not null,
    level        varchar not null
  );
""")

song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs (
    song_id     varchar primary key sortkey,
    title       varchar not null,
    artist_id   varchar not null,
    year        varchar not null,
    duration    float not null
  );
""")

artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artists (
    artist_id     varchar primary key sortkey,
    name          varchar not null,
    location      varchar,
    latitude      varchar,
    longitude     float
  )
""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time (
    start_time timestamp primary key sortkey,
    hour int not null,
    day int not null,
    week int not null,
    month int not null,
    year int not null,
    weekday int not null
  );
""")

# STAGING TABLES
staging_events_copy = ("""
  COPY staging_events FROM '{srcLogData}'
  iam_role '{iamRole}'
  format as json '{logJsonFormat}'
  compupdate off
  truncatecolumns blanksasnull emptyasnull
  timeformat as 'epochmillisecs'
  region 'us-west-2';
""")

staging_songs_copy = ("""
  COPY staging_songs FROM '{srcSongData}'
  iam_role '{iamRole}'
  format as json '{songJsonFormat}'
  compupdate off
  truncatecolumns blanksasnull emptyasnull
  region 'us-west-2';
""")

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
    user_agent
  ) SELECT 
      distinct ts::timestamp,
      e.userId,
      e.level, 
      s.song_id,
      s.artist_id, 
      e.sessionId, 
      s.artist_location,
      e.useragent
    FROM staging_songs s
    JOIN staging_events e ON (
      s.title = e.song 
      AND s.artist_name = e.artist 
      AND s.duration::int = e.length::int
    );
""")

user_table_insert = ("""
  INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
  ) SELECT
      DISTINCT userId AS user_id,
      firstName       AS first_name,
      lastName        AS last_name,
      gender          AS gender,
      level           AS level
    FROM staging_events
    WHERE userId is not null
    AND page = 'NextSong';
""")

song_table_insert = ("""
  INSERT INTO songs (
    song_id, 
    title,
    artist_id, 
    year, 
    duration
  ) SELECT 
      distinct song_id, 
      title, 
      artist_id, 
      year, 
      duration
    FROM staging_songs
    WHERE song_id is not null;
""")

artist_table_insert = ("""
  INSERT INTO artists (
    artist_id, 
    name,
    location,
    latitude, 
    longitude
  ) SELECT 
      distinct artist_id,
      artist_name,
      artist_location,
      artist_latitude,
      artist_longitude
    FROM staging_songs
    WHERE artist_id is not null;
""")

time_table_insert = ("""
  INSERT INTO time (                  
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
  ) SELECT
      distinct ts::timestamp AS start_time,
      extract( hour  FROM start_time ) AS hour,
      extract( day   FROM start_time ) AS day,
      extract( week  FROM start_time ) AS week,
      extract( month FROM start_time ) AS month,
      extract( year  FROM start_time ) AS year,
      extract( week  FROM start_time ) AS weekday
    FROM staging_events
    WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [
  # staging_events_table_create, 
  # staging_songs_table_create, 
  songplay_table_create, 
  user_table_create, 
  song_table_create, 
  artist_table_create, 
  time_table_create
]

drop_table_queries = [
  # staging_events_table_drop, 
  # staging_songs_table_drop, 
  songplay_table_drop, 
  user_table_drop, 
  song_table_drop, 
  artist_table_drop, 
  time_table_drop
]

copy_table_queries = [
  # staging_events_copy, 
  # staging_songs_copy
]

insert_table_queries = [
  songplay_table_insert, 
  user_table_insert, 
  song_table_insert, 
  artist_table_insert, 
  time_table_insert
]
