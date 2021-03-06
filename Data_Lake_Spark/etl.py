import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read song data and process it and save to provided output location
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    # get filepath to song data file
    song_data = 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(input_data+song_data).dropDuplicates()

    # extract columns to create songs table
    df.createOrReplaceTempView('song_data_table')
    songs_table = df.select("song_id","title","artist_id","year","duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs_table.parquet', partitionBy = ('year','artist_id'), mode='overwrite')

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id",'artist_name as name','artist_location as location','artist_latitude as latitude','artist_longitude as longitude')

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table.parquet', mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Read log data and process it and save to provided output location
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df.spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page = 'NextSong')

    # extract columns for users table
    users_table = df.selectExpr('userId as user_id','firstName as first_name','lastName as last_name','gender','level')

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table.parquet', mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp (x // 1000), TimestampType())
    df = df.withColumn('datetime', get_timestamp(col('ts')))

    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df =

    # extract columns to create time table
    time_table = df.selectExpr("datetime as start_time", 'hour(datetime) as hour',
                             'day(datetime) as day', 'weekofyear(datetime) as week',
                             'month(datetime) as month', 'year(datetime) as year',
                            'dayofweek(datetime) as weekday')

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time_table.parquet', partitionBy =('year','month'), mode ='overwrite')

    # read in song data to use for songplays table
    song_df = spark.sql('select * from song_data_table')

    # extract columns from joined song and log datasets to create songplays table
    condition = [song_df.artist_name == df.artist, song_df.title == df.song, song_df.duration == df.length]
    songplays_table = df.join(song_df, condition, 'inner') \
                  .selectExpr ('datetime as start_time', 'userId as user_id','level as level','song_id as song_id',
                               'artist_id as artist_id','sessionId as session_id','artist_location as location',
                               'userAgent as user_agent')
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data +'songplays_table.parquet', partitionBy =('year','month'), mode = 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
