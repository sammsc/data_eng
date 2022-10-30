import configparser
# from datetime import datetime
import os
from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, col
from pyspark.sql.functions import *
# from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('CLUSTER', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('CLUSTER', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    Creates connection to Spark cluster
            Parameters:
                    None
            Returns:
                    spark: connection to Spark cluster
    '''

    spark = SparkSession \
        .builder \
        .appName('Sparkify_Data_Lake') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Load song_data and transform data into fact and dimension tables
            Parameters:
                    spark: spark cluster connection
                    input_data: s3 path to input data files
                    output_data: s3 path to write data
                    
            Returns:
                    df: table of song data
    '''

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', 'A', 'A', 'A')
    # song_data = os.path.join(input_data, 'song_data', '*', '*', '*')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs.parquet'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'))

    # return song data
    return df


def process_log_data(spark, input_data, output_data, song_df):
    '''
    Load log_data and transform data into fact and dimension tables
            Parameters:
                    spark: spark cluster connection
                    input_data: s3 path to input data files
                    output_data: s3 path to write data
                    song_df: table of song data
            Returns:
                    None
    '''

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(["userid", "firstname", "lastname", "gender", "level"]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'))

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    df = df.withColumn("start_time", from_unixtime(df.ts/1000))
    
    # extract columns to create time table
    time_table = df.select('start_time', hour(df.start_time).alias('hour'), dayofmonth(df.start_time).alias('day'), 
                            weekofyear(df.start_time).alias('week'), month(df.start_time).alias('month'), 
                            year(df.start_time).alias('year'), dayofweek(df.start_time).alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time.parquet'), partitionBy=['year', 'month'])

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, on=[df.song == song_df.title, df.artist == song_df.artist_name, df.length == song_df.duration], how='left') \
                        .select('start_time', year('start_time').alias('year'), month('start_time').alias('month'), 'userId', 'level', 'song_id', \
                                'artist_id', 'sessionId', 'location', 'userAgent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), partitionBy=['year', 'month'])


def main():
    """
    - Establishes connection with the Spark cluster and gets session handle to it.  
    
    - Specify s3 path to data.  
    
    - Loads song_data and transforms them into dimension tables.  
    
    - Loads log_data and transforms them into fact and dimension tables. 
    
    - Finally, closes the connection. 
    """

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dlake-sparkify/"
    
    song_df = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, song_df)

    spark.stop()


if __name__ == "__main__":
    main()
