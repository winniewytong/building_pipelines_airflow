from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

start_date = datetime.utcnow()

default_args = {
    'owner': 'Winnie',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 1, 0, 0, 0),
    'end_date': datetime(2020, 6, 10, 0, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}
parent_dag='load_data_s3toredshift'
with DAG(parent_dag,
        default_args=default_args,
        description='Load and transform data in Redshift with Airflow',
        schedule_interval='0 * * * *',
        max_active_runs = 1
        ) as dag:
        
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        table='staging_events',
        s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
        provide_context=True,
        filetype='json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        table='staging_events',
        s3_key="song_data/A/A/A/",
        filetype='json'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="songplays",
        sql_stmt=SqlQueries.songplay_table_insert
    )

    load_users_task = LoadDimensionOperator(
        task_id=f"load_users_dim_table",
        dag=dag,
        table='users',
        truncate=True,
        sql_stmt=SqlQueries.user_table_insert,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials'
    )

    load_artists_task = LoadDimensionOperator(
        task_id=f"load_artists_dim_table",
        dag=dag,
        table='artists',
        truncate=True,
        sql_stmt=SqlQueries.artist_table_insert,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials'
    )

    load_time_task = LoadDimensionOperator(
        task_id=f"load_time_dim_table",
        dag=dag,
        table='time',
        truncate=True,
        sql_stmt=SqlQueries.time_table_insert,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials'
    )

    load_songs_task = LoadDimensionOperator(
        task_id=f"load_songs_dim_table",
        dag=dag,
        table='songs',
        truncate=True,
        sql_stmt=SqlQueries.song_table_insert,
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials'
    )

    data_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
        table=['songs','users','time','artists'],
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials'
    )

    end_operator = DummyOperator(task_id='Stop_execution')

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift] >> \
load_songplays_table >> [load_users_task, load_artists_task, load_time_task, load_songs_task] >>\
data_quality_checks >> end_operator