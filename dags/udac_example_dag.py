from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

"""
This dag will execute the ETL pipeline to extract the data from S3 bucket into Redshift 

Tasks List:
    Staging Task ==> Copy the data from S3 to staging tables
    Load Fact and Dimension Task ==> load the data from staging tables into fact and dimension schema 
    Checking Data Task ==> Data quality check for the schema tables
    
"""

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'airflow pipeline',
    'start_date': datetime(2019, 10, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False 
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/",
    json_path = 's3://udacity-dend/log_json_path.json',
    execution_date="{{ ds }}"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    json_path = 'auto',
    execution_date="{{ ds }}"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    columns="start_time, user_id, level, song_id, artist_id, session_id, location, user_agent",
    query="songplay_table_insert",
    insert_mode="append" 
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    columns="user_id, first_name, last_name, gender, level",
    query="user_table_insert",
    insert_mode="truncate"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    columns="song_id, title, artist_id, year, duration",
    query="song_table_insert",
    insert_mode="truncate"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    columns="artist_id, name, location, latitude, longitude",
    query="artist_table_insert",
    insert_mode="truncate"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    columns="start_time, hour, day, week, month, year, weekday",
    query="time_table_insert",
    insert_mode="truncate"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table 
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator