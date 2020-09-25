In this project, and ETL process is being used to process and transform data stored in an AWS S3 bucket for a mocked up song streaming app.  The raw data files for the app are json log files of events for users streaming music and JSON song files with song information.  Python scripts in Jupyter notebooks are used to compile staging tables from all of the event logs and song files using the 'COPY' functionality in AWS Redshift


Then, and ETL pipeline is created to process the raw data into a star schema for analytical purposes.  The final tables are:
1) songplays - information about song streaming
    songplay_id integer identity(0,1) primary key,
    start_time bigint NOT NULL,
    user_id varchar NOT NULL,
    level text NOT NULL, 
    song_id text, 
    artist_id text,
    session_id varchar NOT NULL,
    location text,
    user_agent text NOT NULL,
        
2) users - user profile information
    user_id varchar,
    first_name text, 
    last_name text, 
    gender text, 
    level text

3) songs - song information
    song_id integer identity(0,1) primary key,
    title text NOT NULL, 
    artist_id text NOT NULL,
    year int NOT NULL, 
    duration real NOT NULL,

4) artist - artist information
    song_id text primary key,
        title text NOT NULL, 
        artist_id text NOT NULL,
        year int NOT NULL, 
        duration real NOT NULL
5) time - information on when songs were streamed


These tables were created to for analytical purposes to derive useful busininess insights. 

Here are some example queries and their expected output:

1)
`SELECT count(*) FROM songplays;`

returns 333

2)
`SELECT count(*) FROM users;`

returns 107

3)
`SELECT count(*) FROM songs;`
returns 14896

4)
`SELECT count(*) FROM artists;`

returns 10025

5)
`SELECT count(*) FROM time;`

returns 8023


