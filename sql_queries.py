import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS \"user\""
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                            create table staging_events (
                                artist nvarchar(200),
                                auth varchar(20),
                                firstName varchar(20),
                                gender char(1),
                                itemInSession int,
                                lastName varchar(40),
                                length double precision,
                                level varchar(10),
                                location  varchar(100),
                                method  varchar(10),
                                page  varchar(20),
                                registration bigint,
                                sessionId bigint,
                                song nvarchar(200),
                                status int,
                                ts bigint,
                                userAgent varchar(600),
                                userId varchar
                            )
""")

staging_songs_table_create = ("""
                                create table staging_songs(
                                    num_songs bigint, 
                                    artist_id varchar(18),
                                    artist_latitude double precision,
                                    artist_longitude double precision,
                                    artist_location varchar(400),
                                    artist_name nvarchar(200),
                                    song_id varchar(18),
                                    title nvarchar(200),
                                    duration double precision,
                                    year int)
""")

songplay_table_create = ("""
                        create table songplay( 
                            songplay_id bigint IDENTITY(0,1) NOT NULL,
                            start_time bigint NOT NULL sortkey distkey, 
                            user_id varchar(18), 
                            level varchar(10) NOT NULL, 
                            song_id varchar(18), 
                            artist_id varchar(18), 
                            session_id bigint, 
                            location varchar(100), 
                            user_agent varchar(600)
                            )
""")

user_table_create = ("""
                        create table \"user\"( 
                            user_id varchar(18) NOT NULL sortkey,
                            first_name varchar(20) NOT NULL,
                            last_name varchar(20) NOT NULL,
                            gender char(1) NOT NULL,
                            level varchar(10) NOT NULL
                            ) diststyle all
""")

song_table_create = ("""
                        create table song( 
                            song_id varchar(18) NOT NULL sortkey,
                            title nvarchar(200) NOT NULL,
                            artist_id varchar(18) NOT NULL, 
                            year int NOT NULL, 
                            duration double precision
                            ) diststyle all
""")

artist_table_create = ("""
                        create table artist( 
                            artist_id varchar(18) NOT NULL sortkey,
                            name nvarchar(200) NOT NULL,
                            location varchar(400),
                            latitude double precision,
                            longitude double precision
                            ) diststyle all
""")

time_table_create = ("""
                        create table time( 
                            start_time bigint NOT NULL sortkey distkey,
                            hour int NOT NULL,
                            day int NOT NULL,
                            week int NOT NULL,
                            month int NOT NULL,
                            year int NOT NULL,
                            weekday int NOT NULL
                            )
""")

# STAGING TABLES
# log-data has camel case named columns, so json path needs to be used when loading data from S3
staging_events_copy = ("""
                        copy staging_events from 's3://{}/log-data.manifest'
                        iam_role '{}'
                        format as json 's3://udacity-dend/log_json_path.json'
                        manifest;
""")

staging_songs_copy = ("""
                        copy staging_songs from 's3://{}/song-data.manifest'
                        iam_role '{}'
                        format as json 'auto'
                        manifest;
""")

# FINAL TABLES

songplay_table_insert = ("""
                            insert into songplay(
                                    start_time, 
                                    user_id, 
                                    level, 
                                    song_id, 
                                    artist_id, 
                                    session_id, 
                                    location, 
                                    user_agent) 
                            select 
                                    se.ts as start_time,
                                    case
                                        when trim(se.userId) ~ '^[0-9]+$' then trim(se.userId)
                                        else null 
                                    end::int as user_id,
                                    se.level,
                                    ss.song_id,
                                    ss.artist_id,
                                    se.sessionId as session_id,
                                    se.location,
                                    se.userAgent
                                from staging_events se left join staging_songs ss
                                    on se.song=ss.title and se.artist=ss.artist_name
                            
""")

user_table_insert = ("""
                        insert into \"user\"( 
                                user_id,
                                first_name,
                                last_name,
                                gender,
                                level)
                        select distinct
                                case
                                    when trim(se.userId) ~ '^[0-9]+$' then trim(se.userId)
                                    else null 
                                end::int as user_id,
                                se.firstName as first_name,
                                se.lastName as last_name,
                                se.gender,
                                se.level                                
                            from staging_events se 
                                WHERE se.userId is not NULL and trim(se.userId) ~ '^[0-9]+$'
                            
""")

song_table_insert = ("""
                        insert into song(
                                song_id,
                                title,
                                artist_id, 
                                year, 
                                duration)
                        select distinct
                                ss.song_id,
                                ss.title,
                                ss.artist_id,
                                ss.year,
                                ss.duration
                            from staging_songs ss
                                WHERE ss.song_id  is not NULL
                                
""")

artist_table_insert = ("""
                        insert into artist( 
                                artist_id,
                                name,
                                location,
                                latitude,
                                longitude)
                        select distinct
                                ss.artist_id,
                                ss.artist_name as name,
                                ss.artist_location as location,
                                ss.artist_latitude as latitude,
                                ss.artist_longitude as longitude
                            from staging_songs ss
                                WHERE ss.artist_id is not NULL
                            
""")

time_table_insert = ("""
                        insert into time( 
                                start_time,
                                hour,
                                day,
                                week,
                                month,
                                year,
                                weekday)
                        select distinct
                                se.ts as start_time,
                                extract('hour' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as hour,
                                extract('day' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as day,
                                extract('week' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as week,
                                extract('month' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as month,
                                extract('year' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as year,
                                extract('weekday' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as weekday
                            from staging_events se
                                WHERE se.ts is not NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
