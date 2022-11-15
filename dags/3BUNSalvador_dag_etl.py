import os
import logging
import boto3
import pandas as pd
from datetime import date, datetime, timedelta
from airflow.models import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(module)s - %(message)s',
                    datefmt='%Y-%m-%d')

logger = logging.getLogger('DAG-Comahue')

default_args = {
    "owner": "P3",
    "retry_delay": timedelta(minutes=60),
}

with DAG(
    'dag_universidad_del_Salvador',
    description='DAG para la Universidad Del Salvador',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 11, 3),
    max_active_runs=5,
    default_args=default_args,
    catchup=False
    
) as dag:

    @task()
    def salvador_extract():
        with open(
            "/home/nvrancovich/airflow/dags/Skill-Up-DA-c-Python/include/P3UNSalvador.sql", "r", encoding="utf-8"
        ) as file:
            query = file.read()
        hook = PostgresHook(postgres_conn_id="alkemy_db")
        df = hook.get_pandas_df(sql=query)
        df.to_csv("/home/nvrancovich/airflow/dags/Skill-Up-DA-c-Python/datasets/3BUNSalvador_select.csv")

    @task
    def salvador_transform():
        pass

    @task
    def salvador_load():
        pass

    salvador_extract() >> salvador_transform() >> salvador_load()