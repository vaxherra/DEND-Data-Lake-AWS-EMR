# Udacity Data Engineering - DATA LAKE ON AWS + EMR

## Introduction


A music streaming startup, Sparkify, has grown their user base and song database wants to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## Project summary


Building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.
 

## Target schemas

### Fact Table

`songplays`  - records in log data associated with song plays i.e. records with page NextSong
> songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
- `users` - users in the app
> user_id, first_name, last_name, gender, level
- `songs` - songs in music database
> song_id, title, artist_id, year, duration
- `artists` - artists in music database
> artist_id, name, location, lattitude, longitude
- `time` - timestamps of records in songplays broken down into specific units
> start_time, hour, day, week, month, year, weekday


# Scripts

- `etl.py` - The script that reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on output S3.


# Configuration

`dl.cfg` contains AWS credentials to access S3 buckets