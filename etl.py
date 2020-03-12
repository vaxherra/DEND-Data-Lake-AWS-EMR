import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Creates or fetches an existing spark session with HADOOP 2.7.0 AWS package.
    
    Args:
        None
    
    Returns:
        spark - a spark session object
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    ELT process for song data. Function extracts information of song data, loads it to a current EMR cluster, and transforms into a pre-defined STAR schema. Finally it saves the set of fact and dimension tables in provided output location.
    
    Params:
        spark       :  Spark Session Object
        input_data  :  input location of the song data files
        output_data :  output location of the processed results
        
    Returns:
        None
    
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json' 
    
    # read song data file
    df = spark.read.json(song_data)
    
    # created 'view' to write SQL Queries
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT sdtn.song_id, 
                            sdtn.title,
                            sdtn.artist_id,
                            sdtn.year,
                            sdtn.duration
                            FROM song_data_table as sdtn
                            WHERE song_id IS NOT NULL
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT arti.artist_id, 
                                arti.artist_name,
                                arti.artist_location,
                                arti.artist_latitude,
                                arti.artist_longitude
                                FROM song_data_table as arti
                                WHERE arti.artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    ELT process for song plays log data. Function extracts information of song plays log data, loads it to a current EMR cluster, and transforms into a pre-defined STAR schema. Finally it saves the set of fact and dimension tables in provided output location.
    
    Args:
        spark       :  Spark Session Object
        input_data  :  input location of the song data files
        output_data :  output location of the processed results
    
    Returns:
        None
    
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df =  spark.read.json(log_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # created log view to write SQL Queries
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table   
    # selecting distinct users (no repeats)
    users_table = spark.sql("""
                            SELECT DISTINCT userT.userId as user_id, 
                            userT.firstName as first_name,
                            userT.lastName as last_name,
                            userT.gender as gender,
                            userT.level as level
                            FROM log_data_table as userT
                            WHERE userT.userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

 
    # A joint SQL query that converts times to timestamp format, where it is not null
    # and then selects fields appropriate to `time_table` table
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                            A.start_time_sub as start_time,
                            hour(A.start_time_sub) as hour,
                            dayofmonth(A.start_time_sub) as day,
                            weekofyear(A.start_time_sub) as week,
                            month(A.start_time_sub) as month,
                            year(A.start_time_sub) as year,
                            dayofweek(A.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(timeSt.ts/1000) as start_time_sub
                            FROM log_data_table timeSt
                            WHERE timeSt.ts IS NOT NULL
                            ) A
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(logT.ts/1000) as start_time,
                                logT.userId as user_id,
                                logT.level as level,
                                songT.song_id as song_id,
                                songT.artist_id as artist_id,
                                logT.sessionId as session_id,
                                logT.location as location,
                                logT.userAgent as user_agent
                                FROM log_data_table logT
                                JOIN song_data_table songT on logT.artist = songT.artist_name and logT.song = songT.title
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://kwapich-udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
