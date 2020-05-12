import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists times;"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events
(
    artist varchar(200),
    auth varchar(50),
    firstName varchar(50),
    gender varchar(50),
    itemInSession int,
    lastName varchar(50),
    length float,
    level varchar(50),
    location varchar(50),
    method varchar(50),
    page varchar(50),
    registration float,
    sessionId int,
    song varchar(200),
    status varchar(50),
    ts bigint,
    userAgent varchar(200),
    userId varchar(50) 
)
diststyle auto;
""")

staging_songs_table_create = ("""
create table if not exists staging_songs
(
    num_songs int,
    artist_id varchar(50),
    artist_latitude float,
    artist_longitude float,
    artist_location varchar(200),
    artist_name varchar(200),
    song_id varchar(50),
    title varchar(200),
    duration float,
    year int
)
diststyle auto;
""")

songplay_table_create = ("""
create table if not exists songplays
(
    songplay_id int identity(1, 1) encode az64 primary key,
    start_time timestamp not null encode az64,
    user_id int not null encode az64,
    level varchar(50) encode zstd,
    song_id varchar(50) not null encode zstd,
    artist_id varchar(50) not null encode zstd,
    session_id int encode az64,
    location_id varchar(50) encode zstd,
    user_agent varchar(200) encode zstd
)
distkey (songplay_id)
sortkey (start_time)
;
""")

user_table_create = ("""
create table if not exists users
(
    user_id int not null encode az64 primary key ,
    first_name varchar(50) encode zstd,
    last_name varchar(50) encode zstd,
    gender char(1) encode zstd,
    level varchar(10) encode zstd 
)
distkey (user_id)
;
""")

song_table_create = ("""
drop table if exists songs;
create table if not exists songs
(
    song_id varchar(50) not null encode zstd primary key,
    title varchar(200) encode zstd,
    artist_id varchar(50) not null encode zstd,
    year int encode az64,
    duration float encode zstd
)
distkey (song_id)
sortkey (year)
;
""")

artist_table_create = ("""
create table if not exists artists
(
    artist_id varchar(50) not null encode zstd primary key,
    artist_name varchar(200) encode zstd,
    artist_location varchar(200) encode zstd,
    artist_latitude float encode zstd,
    artist_longitude float encode zstd
)
distkey (artist_id)
sortkey (artist_id)
;
""")

time_table_create = ("""
create table if not exists times
(
    start_time timestamp not null encode az64 primary key,
    hour int not null encode az64,
    day int not null encode az64,
    week int not null encode az64,
    month int not null encode az64,
    year int not null encode az64,
    weekday int not null encode az64
)
distkey (start_time)
sortkey (start_time)
;
""")

# STAGING TABLES

staging_events_copy = ("""
truncate table staging_events;

copy staging_events
from 's3://udacity-dend/log_data/'
iam_role '{0}'
json 's3://udacity-dend/log_json_path.json';
""").format('arn:aws:iam::475704414487:role/mn-redshift-udacity')
    # config.get('IAM_ROLE', 'arn')

staging_songs_copy = ("""
truncate table staging_songs;

copy staging_songs
from 's3://udacity-dend/song_data/'
iam_role '{0}'
json 'auto';
""").format('arn:aws:iam::475704414487:role/mn-redshift-udacity')
    # config.get('IAM_ROLE', 'arn')

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays
(
    start_time, user_id, level, song_id, 
    artist_id, session_id, location_id, user_agent
)
select distinct
    timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time,
    e.userid::int as user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionid::int as session_id,
    e.location as location_id,
    e.useragent as user_agent
from staging_events e
join staging_songs s 
    on e.song = s.title 
    and e.artist = s.artist_name
where e.page = 'NextSong';
""")

user_table_insert = ("""
insert into users
(
    user_id, first_name, last_name,
    gender, level
)
select
    e.userid::int as user_id,
    e.firstname,
    e.lastname,
    e.gender,
    e.level
from staging_events e
join (
    select max(ts) as ts, userid
    from staging_events
    where page = 'NextSong'
    group by userid
) last_event on last_event.userid = e.userid and last_event.ts = e.ts;
""")

song_table_insert = ("""
insert into songs
(
    song_id, title, artist_id,
    year, duration
)
select
    song_id,
    title,
    artist_id,
    year,
    duration
from staging_songs;
""")

artist_table_insert = ("""
insert into artists
(
    artist_id, artist_name, artist_location,
    artist_latitude, artist_longitude
)
select distinct
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
from staging_songs;
""")

time_table_insert = ("""
insert into times
(
    start_time, hour, day, week,
    month, year, weekday
)
with distinct_times as (
    select distinct start_time
    from songplays
)
select 
    start_time,
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(weekday from start_time) as weekday
from distinct_times;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
