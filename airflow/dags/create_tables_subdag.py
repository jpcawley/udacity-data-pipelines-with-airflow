import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# Returns a DAG which creates a table if it does not exist.
def get_s3_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        create_sql_stmt,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        max_active_runs=1,
        schedule_interval="@once",
        **kwargs
    )

    create_table = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_stmt
    )

    return dag