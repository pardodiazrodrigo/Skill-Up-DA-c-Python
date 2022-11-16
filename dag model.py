#DAG model fully working for google cloud composer
from __future__ import print_function
#sentence before all else to avoid compatibility issues

from datetime import datetime, timedelta

from airflow import models

from airflow.operators import bash_operator
from airflow.operators import python_operator

default_dag_args = {
  #default arguments, setting schedules of 5 retries hourly every day
    'start_date': datetime(2022, 11, 1),
    'retries': 5,
}

with models.DAG(
        'composer_P3HUBA_load',
        schedule_interval=timedelta(minutes=60),
        #catchup=False,
        #template_searchpath= 'gs://us-central1-alkemy-mydata-178a46a9-bucket/dags'
        default_args=default_dag_args) as dag:
    
    #a log message for the dag
    def mss():
        import logging
        logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
        logging.warning('Stamp for this event of log dag3,')


    log_python = python_operator.PythonOperator(
        task_id='log',
        python_callable=mss)

#a simple message to check it is correct

    def greeting():
        import logging
        logging.info('Hello World! This is dag for UBA data from Group 3')

    
    hello_python = python_operator.PythonOperator(
        task_id='hello',
        python_callable=greeting)

    def extract():
        import pandas as pd
        url= '/home/airflow/gcs/dags/G3H_UBA.csv'  #specify csv path
        customers = pd.read_csv(url, sep = ',', encoding_errors= 'ignore')

    
    data_python = python_operator.PythonOperator(
        task_id='dataset',
        python_callable=extract)
    


    goodbye_bash = bash_operator.BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.')
    

    #After defining the task we define their dependencies

    log_python>> hello_python >> data_python>> goodbye_bash
    