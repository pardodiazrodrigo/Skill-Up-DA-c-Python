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
    "ETL_3AUVillaMaria_DagsUnico",
    start_date=datetime(2022, 11, 4),
    max_active_runs=5,
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
    # template_searchpath="C:/Users/GYOKU/airflow/include",
    ) as dag:
        with open('include/SQL_3AUVillaMaria.sql', 'r') as myfile:
            data = myfile.read()

        @task()

        def Extraction():
            #################################### OBTENCION TABLA BASE DE DATOS ################################
            try:
                hook = PostgresHook(postgres_conn_id="alkemy_db")
                df = hook.get_pandas_df(sql=data)
                df.to_csv("files/3AUVillaMaria_select.csv")#####
                logging.info('Tarea de extraccion EXITOSA')
            except:
                logging.info('ERROR al extraer')
            ################################################################################################### 
               
        @task()

        def Transformation():
            try:
                dfSel = pd.read_csv('files/3CUVillaMaria_select.csv', sep=',')

                dfSel = dfSel.astype({'last_name': 'string'})
                dfSel = dfSel.astype({'first_name': 'string'})
                dfSel = dfSel.astype({'university': 'string'})
                dfSel = dfSel.astype({'career': 'string'})
                dfSel = dfSel.astype({'location': 'string'})
                dfSel = dfSel.astype({'email': 'string'})     

            #################################### DIVISION DE CAMPO LAST_NAME ##################################
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("MR._",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("DR._",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("MRS._",""))
            
                dfSel['first_name'] = dfSel['last_name'].map(lambda x:x.split('_') [0])
                dfSel['last_name'] = dfSel['last_name'].map(lambda x:x.split('_') [1])
            ###################################################################################################


            ########################## COMPLETAMINETO CAMPO POSTAL_CODE O LOCATION ############################         
            
                dfCod = pd.read_csv('assets/codigos_postales.csv',sep=',')
                dfCod = dfCod.drop_duplicates(['localidad'], keep='last')
                df = pd.merge(dfSel,dfCod,left_on='location',right_on='localidad')
                del df['localidad']
                del df['postal_code']
                del df['Unnamed: 0']
                df = df.rename(columns={'codigo_postal':'postal_code'})
        
            ###################################################################################################

            ############################## CAMBIO FECHA DE NACIMIENTO POR EDAD ################################
                df["age"] = pd.to_datetime(df["age"], format="%d-%b-%y")

                def datediff(x):
                    today = date.today()
                    age = today.year - x.year - ((today.month, today.day) < (x.month, x.day))
                    return age

                df["age"] = df["age"].apply(datediff)
            ###################################################################################################

            # df["inscription_date"] = pd.to_datetime(df["inscription_date"], format="%d-%b-%y")

            ################################# FILTRACION POR EDAD #############################
                df = df.loc[df["age"].between(18, 90)]
            ###################################################################################

            #################################### NORMALIZACION #################################
            
                df["university"] = df["university"].apply(lambda x: x.replace("_"," "))
                df["career"] = df["career"].apply(lambda x: x.replace("_"," "))
                df["location"] = df["location"].apply(lambda x: x.replace("_"," "))

                df["university"] = df["university"].str.lower()
                df["career"] = df["career"].str.lower()
                df["location"] = df["location"].str.lower()
                df["first_name"] = df["first_name"].str.lower()
                df["last_name"] = df["last_name"].str.lower()
                df["email"] = df["email"].str.lower()

                df = df.replace({'gender': {'F':'female','M':'male'}})

                df = df.reindex(columns= ['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email'])

                df = df.astype({'inscription_date': 'string'})
                df = df.astype({'first_name': 'string'})
                df = df.astype({'last_name': 'string'})
                df = df.astype({'gender': 'string'})
                df = df.astype({'postal_code': 'string'})
                df = df.astype({'location': 'string'}) 

            # del df['Unnamed: 0']
            ###################################################################################################
            
                df.to_csv("datasets/3AUVillaMaria_process.txt", sep="\t", index=None)

                logging.info('Tarea de transformacion EXITOSA')
            except:
                logging.info('ERROR al transformar')
        
        @task()

        def Load():
            try:
                ACCESS_KEY = "AKIAY27PJEHOPCMGIA7C"
                SECRET_ACCESS_KEY = "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"
                session = boto3.Session(
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_ACCESS_KEY,
                )
                s3 = session.resource("s3")
                data = open("datasets/3AUVillaMaria_process.txt", "rb")#####
                s3.Bucket("alkemy-p3").put_object(
                    Key="preprocess/3AUVillaMaria_process.txt", Body=data
                )

                logging.info('Tarea de carga EXITOSA')
            except:
                logging.info('ERROR al cargar')

        Extraction() >> Transformation() >> Load()