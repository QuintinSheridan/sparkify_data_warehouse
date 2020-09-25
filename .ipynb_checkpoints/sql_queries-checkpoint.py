import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        num_events integer identity(0,1),
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession varchar,
        lastName varchar,
        length varchar,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId varchar,
        song varchar,
        status varchar,
        ts bigint,
        userAgent varchar,
        userId varchar
    );
""")



staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        artist_id varchar,
        artist_latitude real,
        artist_longitude real, 
        artist_location varchar,
        artist_name varchar, 
        duration real,
        num_songs int,
        song_id varchar,
        title varchar,
        year INT   
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id integer identity(0,1) primary key,
        start_time bigint NOT NULL,
        user_id varchar NOT NULL,
        level text, 
        song_id varchar NOT NULL, 
        artist_id varchar NOT NULL,
        session_id varchar,
        location text,
        user_agent text,
        unique(start_time, user_id, level, 
            song_id, artist_id, session_id,
            location, user_agent)
    );
""")


user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id varchar PRIMARY KEY,
        first_name text, 
        last_name text, 
        gender text, 
        level text
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar primary key,
        title text, 
        artist_id varchar NOT NULL,
        year int, 
        duration real,
        unique(title, artist_id, year, duration)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar, 
        longitude real,
        lattitude real
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY,
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL
    );
""")


staging_events_copy = ("""
    COPY staging_events
    FROM {path}
    IAM_ROLE {iam_role}
    REGION 'us-west-2'
    JSON {json}
    """.format(path= config.get("S3","LOG_DATA") , 
               iam_role = config.get("IAM_ROLE", "ARN"), 
               json = config.get("S3", "LOG_JSONPATH"))
    )


staging_songs_copy = ("""
    COPY staging_songs
    FROM {path}
    IAM_ROLE {iam_role}
    REGION 'us-west-2'
    JSON 'auto'
""".format(path = config.get("S3","SONG_DATA") , 
           iam_role = config.get("IAM_ROLE", "ARN")
        )   
    )


# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, 
        song_id, artist_id, session_id, location, user_agent)
    (SELECT
        se.ts, 
        se.userId, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionId, 
        se.location, 
        se.userAgent
    FROM 
        staging_events AS se INNER JOIN staging_songs AS ss 
        on se.artist = ss.artist_name AND se.song=ss.title
    WHERE
        page='NextSong');
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    (SELECT 
        DISTINCT userId, 
        firstName, 
        lastName, 
        gender, 
        level
    FROM
        staging_events);
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    (SELECT 
        DISTINCT song_id,
        title, 
        artist_id, 
        year, 
        duration
    FROM 
        staging_songs);
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, lattitude, longitude)
    (SELECT 
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM
        staging_songs);
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    (SELECT 
        DISTINCT ts.start_time,
        EXTRACT(hour FROM ts.start_time) AS hour,
        EXTRACT(day FROM ts.start_time) AS day,
        EXTRACT(week FROM ts.start_time) AS week,
        EXTRACT(month FROM ts.start_time) AS month,
        EXTRACT(year FROM ts.start_time) AS year,
        EXTRACT(weekday FROM ts.start_time) AS weekday
    FROM
        (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' 
        as start_time FROM staging_events) AS ts);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


