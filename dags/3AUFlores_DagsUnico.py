from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
# import os
from airflow.decorators import task
import pandas as pd
import csv
import boto3
import logging
args = {"owner": "P3"}

default_args = {
    "owner": "P3",
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 60 minutes
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}


# Instantiate DAG

with DAG(
    "ETL_3AUFlores_DagsUnico",
    start_date=datetime(2022, 11, 4),
    max_active_runs=5,
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
    # template_searchpath="C:/Users/GYOKU/airflow/include",
    ) as dag:
        with open('include/SQL_3AUFlores.sql', 'r') as myfile:
            data = myfile.read()
        @task()

        def Extraction():
            #################################### OBTENCION TABLA BASE DE DATOS ################################
            try:
                hook = PostgresHook(postgres_conn_id="alkemy_db")
                df = hook.get_pandas_df(sql=data)
                df.to_csv("files/3AUFlores_select.csv")
                logging.info('Tarea de extraccion EXITOSA')
            except:
            ###################################################################################################
                logging.info('ERROR al extraer')
        @task()

        def Transformation():
            pass
        @task()

        def Load():
            pass

        Extraction() >> Transformation() >> Load()