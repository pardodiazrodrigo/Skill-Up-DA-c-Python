# [START composer_simple]
from __future__ import print_function
#sentence before all else


from datetime import datetime, timedelta

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator


from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator


default_dag_args = {
  
    'start_date': datetime(2022, 11, 1),
    'retries': 5,
}

with models.DAG(
        'composer_P3HUBA_etload',
        schedule_interval=timedelta(minutes=60),
        #catchup=False,
        #template_searchpath= 'gs://us-central1-alkemy-mydata-178a46a9-bucket/dags'
        default_args=default_dag_args) as dag:
    

    def mss():
        import logging
        logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
        logging.warning('Stamp for this event of log dag3,')


    log_python = python_operator.PythonOperator(
        task_id='log',
        python_callable=mss)


    def greeting():
        import logging
        logging.info('Hello World! This is dag for UBA data from Group 3')


    hello_python = python_operator.PythonOperator(
        task_id='hello',
        python_callable=greeting)

    def etload():
        import pandas as pd
        url= '/home/airflow/gcs/dags/G3H_UBA.csv'  #specify csv path
        df = pd.read_csv(url, sep = ',', encoding_errors= 'ignore')
        df.columns = ['last_name', 'gender', 'address',	'emails', 'birth_date', 'universities', 'inscription_date', 'careers', 'zipcodes']
        
        ###################################################################################################
        df.dtypes.to_csv("/home/airflow/gcs/dags/G3HUBA_dtypes.csv")
        
        df.to_csv("/home/airflow/gcs/dags/G3HUBA_select.csv")
        df.to_csv(
            "/home/airflow/gcs/dags/G3HUBA_process.txt", sep="\t", index=None
        )


    data_python = python_operator.PythonOperator(
        task_id='dataset',
        python_callable=etload)
    

    ######Cloud composer requires an operator for downloading the files. 
    # Unfortunately the task failed

    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name='G3HUBA_select.csv',
        bucket='us-central1-alkemy-mydata-178a46a9-bucket/dags/',
        filename='/home/airflow/gcs/dags/G3HUBA_select.csv',
    )

    goodbye_bash = bash_operator.BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.')
   
    log_python>> hello_python >> data_python>> download_file >> goodbye_bash
   