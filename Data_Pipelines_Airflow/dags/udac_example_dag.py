from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay':timedelta(minutes=3),
    'catchup':False,
    'email_on_retry':False,
    'start_date': datetime(2019, 1, 12)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval = '@once',
          #schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_events_table = PostgresOperator(
    task_id='create_table_staging_events',
    postgres_conn_id='redshift',
    sql=SqlQueries.create_staging_events,
    dag=dag
)
create_songs_table = PostgresOperator(
    task_id='create_table_songs',
    postgres_conn_id='redshift',
    sql=SqlQueries.create_staging_songs,
    dag=dag
)
create_songplays_table = PostgresOperator(
    task_id='create_songplays_table',
    postgres_conn_id='redshift',
    sql=SqlQueries.create_songplay_table,
    dag=dag
)

create_users_table = PostgresOperator(
    task_id='create_users_table',
    postgres_conn_id='redshift',
    sql=SqlQueries.create_users_table,
    dag=dag
)
create_songs_table_dimension = PostgresOperator(
    task_id='create_songs_table_dimension',
    postgres_conn_id='redshift',
    sql=SqlQueries.create_songs_table,
    dag=dag
)
create_artists_table = PostgresOperator(
    task_id='create_artists_table',
    postgres_conn_id='redshift',
    sql=SqlQueries.create_artists_table,
    dag=dag
)
create_time_table = PostgresOperator(
    task_id='create_time_table',
    postgres_conn_id='redshift',
    sql=SqlQueries.create_time_table,
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_paths ='log_json_path.json'
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    provide_context=True,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='users',
    provide_context=True,
    sql=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    provide_context=True,
    sql=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    provide_context=True,
    sql=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    provide_context=True,
    sql=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks_songplays = DataQualityOperator(
    task_id='Run_data_quality_checks_songplays',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays'
)
run_quality_checks_songs = DataQualityOperator(
    task_id='Run_data_quality_checks_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs'
)
run_quality_checks_users = DataQualityOperator(
    task_id='Run_data_quality_checks_users',
    dag=dag,
    redshift_conn_id='redshift',
    table='users'
)
run_quality_checks_time = DataQualityOperator(
    task_id='Run_data_quality_checks_time',
    dag=dag,
    redshift_conn_id='redshift',
    table='time'
)
run_quality_checks_artists = DataQualityOperator(
    task_id='Run_data_quality_checks_artists',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


#Task dependencies
start_operator >> create_events_table
start_operator >> create_songs_table
start_operator >> create_songplays_table
start_operator >> create_songs_table_dimension
start_operator >> create_users_table
start_operator >> create_artists_table
start_operator >> create_time_table
create_events_table >> stage_events_to_redshift
create_songs_table >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks_users
load_song_dimension_table >> run_quality_checks_songs
load_artist_dimension_table >> run_quality_checks_artists
load_time_dimension_table >> run_quality_checks_time
load_songplays_table >> run_quality_checks_songplays
run_quality_checks_songplays >> end_operator
run_quality_checks_songs >> end_operator
run_quality_checks_artists >> end_operator
run_quality_checks_users >> end_operator
run_quality_checks_time >> end_operator