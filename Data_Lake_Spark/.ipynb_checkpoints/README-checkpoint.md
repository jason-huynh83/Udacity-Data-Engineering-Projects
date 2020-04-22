# Sparkify's Data Lake
## Introduction
- Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app
- Project creates analytics model using song and log files in S3 Bucket using spark on an EMR Cluster and writes the result to S3

## Project Description
Apply the knowledge of Spark and Data Lakes to build an ETL pipeline for a Data Lake hosted on Amazon S3

In this task, we have to build an ETL pipeline that extracts their data from S3 and process them using Spark and then load back into S3 in a set of Fact and Dimension tables. This will allow their analytics team to continue finding insights in what songs their users are listening. In addition, you will have to deploy this Spark process on a Cluster using AWS.
## Data Schema and Design
### Fact Table
- **songplays** - records in log data associated with song plays i.e. records with page NextSong.
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
 
 ### Dimension Tables
 - **users** - users in the app
   - user_id, first_name, last_name, gender, level
  - **time** - timestamps of records in songplays broekn down into specific units
    - start_time, hour, day, week, month, year, weekday
 - **artists** - artists in music database
   - artist_id, name, location, latitude, longitude
  - **songs** - songs in the music database
    - song_id, title, artist_id, year, duration
   
 ![](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/33760/1586916755/Song_ERD.png)

## Setup
**ETL Pipeline**

1.  Load the credentials from dl.cfg
2.  Load the Data which are in JSON Files(Song Data and Log Data)
3.  After loading the JSON Files from S3 
4.  Use Spark process this JSON files and then generate a set of Fact and Dimension Tables
5.  Load back these dimensional process to S3

**Final Instructions**

1.  Write correct keys in dl.cfg
2.  Run in a Jupyter Notebook in EMR Cluster to ensure the correct tables are returned
3.  Should take about 2-4 mins in total