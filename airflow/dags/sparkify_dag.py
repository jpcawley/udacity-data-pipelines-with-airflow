from datetime import datetime, timedelta
import os
# import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from airflow.sensors import ExternalTaskSensor
from helpers import SqlQueries

from create_tables_subdag import get_s3_to_redshift_dag

import create_table_stmts

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
start_date = datetime.utcnow()

default_args = {
    'owner': 'JPCawley',
    'start_date': start_date,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          # schedule_interval='@once'
          )

create_tables_task = ExternalTaskSensor(
    task_id="trigger_create_tables_dag",
    external_dag_id='create_tables_dag'
    execution_delta=timedelta(minutes=-5),
    allowed_states=['success', 'failed', 'skipped'],
    failed_states=['failed', 'skipped'],
    start_date=datetime(2018, 1, 11),
    timeout=120,
    # soft_fail=True,
)

# create_staging_events = SubDagOperator(
#     subdag=get_s3_to_redshift_dag(
#         'sparkify_dag',
#         'create_staging_events_table',
#         'redshift',
#         'aws_credentials',
#         'staging_events',
#         create_table_stmts.CREATE_STAGING_EVENTS_TABLE,
#         start_date=start_date,
#     ),
#     task_id='create_staging_events_table',
#     dag=dag,
#     retries= 1,
# )

# create_staging_songs = SubDagOperator(
#     subdag=get_s3_to_redshift_dag(
#         'sparkify_dag',
#         'create_staging_songs_table',
#         'redshift',
#         'aws_credentials',
#         'staging_events',
#         create_table_stmts.CREATE_STAGING_SONGS_TABLE,
#         start_date=start_date,
#     ),
#     task_id='create_staging_songs_table',
#     dag=dag
# )

# create_songplays = SubDagOperator(
#     subdag=get_s3_to_redshift_dag(
#         'sparkify_dag',
#         'create_songplays_table',
#         'redshift',
#         'aws_credentials',
#         'staging_events',
#         create_table_stmts.CREATE_SONGPLAYS_TABLE,
#         start_date=start_date,
#     ),
#     task_id='create_songplays_table',
#     dag=dag
# )

# create_songs = SubDagOperator(
#     subdag=get_s3_to_redshift_dag(
#         'sparkify_dag',
#         'create_songs_table',
#         'redshift',
#         'aws_credentials',
#         'staging_events',
#         create_table_stmts.CREATE_SONGS_TABLE,
#         start_date=start_date,
#     ),
#     task_id='create_songs_table',
#     dag=dag
# )

# create_artists = SubDagOperator(
#     subdag=get_s3_to_redshift_dag(
#         'sparkify_dag',
#         'create_artists_table',
#         'redshift',
#         'aws_credentials',
#         'staging_events',
#         create_table_stmts.CREATE_ARTISTS_TABLE,
#         start_date=start_date,
#     ),
#     task_id='create_artists_table',
#     dag=dag
# )

# create_users = SubDagOperator(
#     subdag=get_s3_to_redshift_dag(
#         'sparkify_dag',
#         'create_users_table',
#         'redshift',
#         'aws_credentials',
#         'staging_events',
#         create_table_stmts.CREATE_USERS_TABLE,
#         start_date=start_date,
#     ),
#     task_id='create_users_table',
#     dag=dag
# )

# create_time = SubDagOperator(
#     subdag=get_s3_to_redshift_dag(
#         'sparkify_dag',
#         'create_time_table',
#         'redshift',
#         'aws_credentials',
#         'staging_events',
#         create_table_stmts.CREATE_TIME_TABLE,
#         start_date=start_date,
#     ),
#     task_id='create_time_table',
#     dag=dag
# )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create_tables_task = PostgresOperator(
#     task_id = 'create_postgres_tables',
#     dag=dag,
#     postgres_conn_id = 'redshift',
#     sql = create_tables)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    table='staging_events',
    region='us-west-2',
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data/A/B/C/',
    table='staging_songs',
    region='us-west-2',
    extra_params="JSON 'auto' COMPUPDATE OFF"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    truncate=True,
    sql_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    truncate=True,
    sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    truncate=True,
    sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    truncate=True,
    sql_stmt=SqlQueries.time_table_insert
)

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# start_operator >> create_tables_task >> stage_events_to_redshift >> stage_songs_to_redshift >> end_operator
# start_operator >> stage_events_to_redshift >> stage_songs_to_redshift >> end_operator

# start_operator >> create_staging_events
# start_operator >> create_staging_songs
# start_operator >> create_songplays
# start_operator >> create_songs
# start_operator >> create_artists
# start_operator >> create_users
# start_operator >> create_time
start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift
# create_staging_events >> stage_events_to_redshift
# create_staging_events >> stage_songs_to_redshift
# create_staging_songs >> stage_events_to_redshift
# create_staging_songs >> stage_songs_to_redshift
# create_songplays >> stage_events_to_redshift
# create_songplays >> stage_songs_to_redshift
# create_songs >> stage_events_to_redshift
# create_songs >> stage_songs_to_redshift
# create_artists >> stage_events_to_redshift
# create_artists >> stage_songs_to_redshift
# create_users >> stage_events_to_redshift
# create_users >> stage_songs_to_redshift
# create_time >> stage_events_to_redshift
# create_time >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table >> end_operator
load_songplays_table >> load_artist_dimension_table >> end_operator
load_songplays_table >> load_user_dimension_table >> end_operator
load_songplays_table >> load_time_dimension_table >> end_operator
# load_time_dimension_table >> end_operator
