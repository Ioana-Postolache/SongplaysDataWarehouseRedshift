# SongplaysDataWarehouseRedshift
This project builds a data warehouse in AWS Redshift with data for a fictional song play app , using data that resides in S3.

The data warehouse data comes from two datasets that reside in S3. Below are the S3 links for each:

- Song data: s3://udacity-dend/song_data - contains JSON metadata on the songs in the app
- Log data: s3://udacity-dend/log_data - contains JSON logs on user activity on the app
Log data json path: s3://udacity-dend/log_json_path.json

The data is loaded from S3 to staging tables on Redshift and SQL statements are executed to create the the tables below from these staging tables:
- Fact Table
* songplays - records in event data associated with song plays i.e. records with page NextSong; 
Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- Dimension Tables
* users - users in the app;
Columns: user_id, first_name, last_name, gender, level
* songs - songs in music database;
Columns: song_id, title, artist_id, year, duration
* artists - artists in music database;
Columns: artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units;
Columns: start_time, hour, day, week, month, year, weekday

## How to run
Run the following scripts - the ones that have the same numbers can be run in parallel
- 1) iac.py - to create Redshift cluster and role
- 1) create_manifest_files.py - to create manifest files for S3 data
- 2) create_tables.py - creates the 5 tables mentioned abode
- 3) etl.py -connects to the redshift database, loads log_data and song_data into staging tables, and transforms them into the five tables.
- 4) clean_up_resources.py - to delete cluster and role

## Song Dataset
The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
- {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

Below is an example of what the data in a log file, 2018-11-12-events.json, looks like.
![ 2018-11-12-events.json](https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/58ff61b9-a54f-496d-b4c7-fa22750f6c76/lessons/b3ce1791-9545-4187-b1fc-1e29cc81f2b0/concepts/fa049d13-5e15-4333-b909-f1f6f0ce36a5# "Log data")

## Log Dataset
The second dataset consists of log files in JSON format generated by [this event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset are partitioned by year and month.


## Config file (dwh.cfg) structure
```
[CLUSTER]
host = ''
db_name = ''
db_user = ''
db_password = ''
db_port = ''

[IAM_ROLE]
arn =''

[S3]
log_data = ''
log_jsonpath = ''
song_data = ''
s3_personal_bucket = ''

[AWS]
key = ''
secret = ''

[DWH]
dwh_cluster_type = ''
dwh_num_nodes = ''
dwh_node_type = ''
dwh_iam_role_name = ''
dwh_cluster_identifier = ''
dwh_db = ''
dwh_db_user = ''
dwh_db_password = ''
dwh_port = ''
dwh_endpoint = ''
```
