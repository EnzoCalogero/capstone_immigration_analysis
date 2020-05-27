from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#
from airflow.operators import (LoadFactOperator, StageToRedshiftOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020,5,10), #datetime(2020, 1, 12),
    'depends_on_past': False,
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry': False
}

dag = DAG('udac_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_source="s3://udacity-dend/log_data",
    
    json_paths="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_source="s3://udacity-dend/song_data/", #A/A/A",
    json_paths="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    insert_select_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    insert_select_query=SqlQueries.user_table_insert,
    insert_after_delete=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    insert_select_query=SqlQueries.song_table_insert,
    insert_after_delete=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    insert_select_query=SqlQueries.artist_table_insert,
    insert_after_delete=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    insert_select_query=SqlQueries.time_table_insert,
    insert_after_delete=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    
    # list of tests with the expected result
    list_tests={
    "SELECT COUNT(*) FROM songs":14896,
    "SELECT COUNT(*) FROM artists":10025,
    "SELECT COUNT(*) FROM users":104,
    "SELECT COUNT(*) FROM time":6820,
    "SELECT COUNT(*) FROM songplays":6820
    },
    tables=(
        'songs',
        'artists',
        'users',
        'time',
        'songplays'
    )
)

end_operator = DummyOperator(task_id='Stop_execution',
                             dag=dag
                            )

# Airflow task sequence and dependencies
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
