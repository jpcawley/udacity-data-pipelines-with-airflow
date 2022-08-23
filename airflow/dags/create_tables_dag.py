from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.hooks.postgres_hook import PostgresHook
import create_table_stmts

start_date = datetime.utcnow()

default_args = {
    'owner': 'JPCawley',
    'start_date': start_date,
    'depends_on_past': False,
    'max_active_runs':1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('create_tables_dag',
          default_args=default_args,
          description='Create staging tables, fact table, and dimension tables.',
          schedule_interval='0 * * * *'
      )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_events = PostgresOperator(
    task_id='create_staging_events_table',
    dag=dag,
    table='staging_events',
    postgres_conn_id='redshift',
    sql=create_table_stmts.CREATE_STAGING_EVENTS_TABLE,
)

create_staging_songs = PostgresOperator(
    task_id='create_staging_songs_table',
    dag=dag,
    table='staging_songs',
    postgres_conn_id='redshift',
    sql=create_table_stmts.CREATE_STAGING_SONGS_TABLE,
)

create_songs = PostgresOperator(
    task_id='create_songs_table',
    dag=dag,
    table='songs',
    postgres_conn_id='redshift',
    sql=create_table_stmts.CREATE_SONGS_TABLE,
)

create_songplays = PostgresOperator(
    task_id='create_songplays_table',
    dag=dag,
    table='songplays',
    postgres_conn_id='redshift',
    sql=create_table_stmts.CREATE_SONGPLAYS_TABLE,
)

create_users = PostgresOperator(
    task_id='create_users_table',
    dag=dag,
    table='users',
    postgres_conn_id='redshift',
    sql=create_table_stmts.CREATE_USERS_TABLE,
)

create_artists = PostgresOperator(
    task_id='create_artists_table',
    dag=dag,
    table='artists',
    postgres_conn_id='redshift',
    sql=create_table_stmts.CREATE_ARTISTS_TABLE,
)

create_time = PostgresOperator(
    task_id='create_time_table',
    dag=dag,
    table='time',
    postgres_conn_id='redshift',
    sql=create_table_stmts.CREATE_TIME_TABLE,
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

start_operator >> [create_staging_events, create_staging_songs, create_songplays, create_songs, create_users, create_artists, create_time] >> end_operator
# start_operator >> create_staging_songs
# start_operator >> create_songplays
# start_operator >> create_songs
# start_operator >> create_users
# start_operator >> create_artists
# start_operator >> create_time
# [create_staging_events, create_staging_songs, create_songplays, create_songs, create_users, create_artists, create_time] >> end_operator