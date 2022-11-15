import boto3
import pandas as pd
from datetime import date, datetime, timedelta
from airflow.models import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task

default_args = {
    "owner": "P3",
    "retry_delay": timedelta(minutes=60),
}

with DAG(
    'dag_universidad_del_Comahue',
    description='DAG para la Universidad Nacional Del Comahue',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 11, 3),
    max_active_runs=5,
    default_args=default_args,
    catchup=False
    
) as dag:

    @task()
    def comahue_extract():
        pass

    @task
    def comahue_transform():
        pass

    @task
    def comahue_load():
        pass

    comahue_extract() >> comahue_transform() >> comahue_load()