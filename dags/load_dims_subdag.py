import datetime

from airflow import DAG
from airflow.operators import (LoadDimensionOperator, DataQualityOperator)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries

def get_fact_to_dims_dag(
        parent_dag_name,
        task_id,
        table,
        truncate,
        sql_stmt,
        redshift_conn_id,
        aws_credentials_id,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    """
    There are two asks in this subdag. 
    First, the load_tasks dag will load data from fact
    to different dimension table. 
    Then, the check_task will conduct a data quality 
    check for each table. 
    """
    load_task = LoadDimensionOperator(
        task_id=f"load_{table}_table",
        dag=dag,
        table=table,
        truncate=truncate,
        insert_sql=sql_stmt,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id
)

    check_task = DataQualityOperator(
        task_id=f"check_{table}_data",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
)
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

    load_task >> check_task

    return dag
