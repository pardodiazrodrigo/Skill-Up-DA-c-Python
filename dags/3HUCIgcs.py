from __future__ import print_function
#sentence before all else

#import libraries

import csv
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
        '3HUCIextractcomposer',
        schedule_interval=timedelta(minutes=60),
        #catchup=False,
        #template_searchpath= 'gs://us-central1-alkemy-mydata-178a46a9-bucket/dags'
        ##path for composer bucket could be included
        default_args=default_dag_args) as dag:
  
#set log message
    def mss():
        import logging
        logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
        logging.warning('Stamp for this event of log dag3,')


    log_python = python_operator.PythonOperator(
        task_id='log',
        python_callable=mss)


    def greeting():
        import logging
        logging.info('Hello World! This is dag for UCine data from Group 3')

    
    hello_python = python_operator.PythonOperator(
        task_id='hello',
        python_callable=greeting)

    def etload():
        import pandas as pd
        url= '/home/airflow/gcs/dags/G3H_UCI.csv'  #specify csv path
        df = pd.read_csv(url, sep = ',', encoding_errors= 'ignore')
        
        df['universities']= df['universities'].astype(str)
        df['names']= df['names'].astype(str)
        df['careers'] = df['careers'].astype(str)
        df['emails']= df['emails'].astype(str)
        df['gender']= df['gender'].astype(str)
        df['address']= df['address'].astype(str)
        df['locations']= df['locations'].astype(str)

        df['universities']= df['universities'].apply(lambda x: x.lower())
        df['names']= df['names'].apply(lambda x: x.lower())
        df['careers'] = df['careers'].apply(lambda x: x.lower())
        df['emails']= df['emails'].apply(lambda x: x.lower())
        df['gender']= df['gender'].apply(lambda x: x.lower())
        df['address']= df['address'].apply(lambda x: x.lower())

        df['universities']= df['universities'].apply(lambda x: x.strip())
        df['names']= df['names'].apply(lambda x: x.strip())
        df['careers'] = df['careers'].apply(lambda x: x.strip())
        df['emails']= df['emails'].apply(lambda x: x.strip())
        df['gender']= df['gender'].apply(lambda x: x.strip())
        df['address']= df['address'].apply(lambda x: x.strip())
        df['locations']= df['locations'].apply(lambda x: x.strip())


        df['inscription_dates']= pd.Series( df['inscription_dates'])
        df['inscription_dates'] = df['inscription_dates'].apply(lambda x: datetime.strptime(x, "%d-%m-%Y"))
        df['birth_dates']= pd.Series( df['birth_dates'])
        df['birth_dates'] = df['birth_dates'].apply(lambda x: datetime.strptime(x, "%d-%m-%Y"))

        
        df.columns = ['last_name', 'gender', 'address',	'emails', 'birth_date', 'universities', 'inscription_date', 'careers', 'locations']
        
        ###################################################################################################
        df.dtypes.to_csv("/home/airflow/gcs/dags/G3HUCI_dtypes.csv")
        #################################### EXPORTACION CSV ##############################################
        df.to_csv("/home/airflow/gcs/dags/G3HUCI_select.csv")
        df.to_csv(
            "/home/airflow/gcs/dags/G3HUCI_process.txt", sep="\t", index=None
        )

    
    data_python = python_operator.PythonOperator(
        task_id='dataset',
        python_callable=etload)
    
    goodbye_bash = bash_operator.BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.')
   
    log_python>> hello_python >> data_python>> goodbye_bash