from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTableOperator)
from helpers import SqlQueries
import create_table_stmts

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

REDSHIFT_CONN_ID='redshift'
AWS_CREDENTIALS_ID='aws_credentials'
S3_BUCKET='udacity-dend'
S3_KEY_LOG='log_data'
S3_KEY_SONG='song_data'
REGION='us-west-2'

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
          description='Create table, load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
      )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'artists', 'songs', 'time']
qrys = [create_table_stmts.CREATE_STAGING_EVENTS_TABLE, create_table_stmts.CREATE_STAGING_SONGS_TABLE, create_table_stmts.CREATE_SONGPLAYS_TABLE, create_table_stmts.CREATE_USERS_TABLE, create_table_stmts.CREATE_ARTISTS_TABLE, create_table_stmts.CREATE_SONGS_TABLE, create_table_stmts.CREATE_TIME_TABLE]
create_tasks = []
    
for table, qry in zip(tables, qrys):
    task = CreateTableOperator(
        task_id=f'create_{table}_table',
        dag=dag,
        table=table,
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql_stmt=qry,
        delete=True
    )
    
    create_tasks.append(task)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_LOG,
    table='staging_events',
    region=REGION,
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"  
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_SONG,
    table='staging_songs',
    region=REGION,
    extra_params = "JSON 'auto' COMPUPDATE OFF"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql_stmt=SqlQueries.songplay_table_insert
)


dim_tables = ['users', 'artists', 'songs', 'time']
dim_qrys = [SqlQueries.user_table_insert, SqlQueries.artist_table_insert, SqlQueries.song_table_insert, SqlQueries.time_table_insert]
dim_tasks = []
    
for table, qry in zip(dim_tables, dim_qrys):
    task = LoadDimensionOperator(
        task_id=f'load_{table}_dim_table',
        dag=dag,
        table=table,
        redshift_conn_id=REDSHIFT_CONN_ID,
        truncate=True,
        sql_stmt=qry
    )
    
    dim_tasks.append(task)

data_quality_qrys = [
    {'qry': 'SELECT COUNT(*) FROM songplays WHERE userid IS NULL', 'expected_result': 0}, 
    {'qry': 'SELECT COUNT(*) FROM users WHERE user_id IS NULL', 'expected_result': 0}, 
    {'qry': 'SELECT COUNT(*) FROM artists WHERE artist_id IS NULL', 'expected_result': 0}, 
    {'qry': 'SELECT COUNT(*) FROM songs WHERE title IS NULL', 'expected_result': 0}, 
    {'qry': 'SELECT COUNT(*) FROM time WHERE hour IS NULL', 'expected_result': 0}, 
    {'qry': 'SELECT COUNT(*) FROM songplays WHERE level NOT IN ("paid", "free")', 'expected_result': 0}, 
    {'qry': 'SELECT COUNT(DISTINCT weekday) FROM time', 'expected_result': 7}
]

run_quality_checks = DataQualityOperator(
    task_id='run_quality_checks',
    dag=dag,
    data_quality_checks=data_quality_qrys,
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tasks
create_tasks >> stage_events_to_redshift
create_tasks >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table >> dim_tasks >> run_quality_checks >> end_operator
