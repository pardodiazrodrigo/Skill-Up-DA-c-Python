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
            try:
            
                dfSel = pd.read_csv('files/3CUFlores_select.csv', sep=',')

                dfSel = dfSel.astype({'last_name': 'string'})
                dfSel = dfSel.astype({'first_name': 'string'})
                dfSel = dfSel.astype({'university': 'string'})
                dfSel = dfSel.astype({'career': 'string'})
                dfSel = dfSel.astype({'location': 'string'})
                dfSel = dfSel.astype({'email': 'string'}) 
                # dfSel = dfSel.astype({'postal_code': 'string'})

                
                #################################### DIVISION DE CAMPO LAST_NAME #################################
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("MR. ",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("DR. ",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("MRS. ",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" MD",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" DDS",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" PHD",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" DVM",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" JR.",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" II",""))
                dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" IV",""))
                
                dfSel['first_name'] = dfSel['last_name'].map(lambda x:x.split(' ') [0])
                dfSel['last_name'] = dfSel['last_name'].map(lambda x:x.split(' ') [1])
                ###################################################################################################

                ########################## COMPLETAMINETO CAMPO POSTAL_CODE O LOCATION ############################         
                dfCod = pd.read_csv('assets/codigos_postales.csv',sep=',')
                dfCod = dfCod.drop_duplicates(['localidad'], keep='last')
                df = pd.merge(dfSel,dfCod,left_on='postal_code',right_on='codigo_postal')
                del df['codigo_postal']
                del df['location']
                del df['Unnamed: 0']
                df = df.rename(columns={'localidad':'location'})
                ###################################################################################################
                #df.to_csv("/usr/local/airflow/include/3CUFlores_select_final.csv")
                ############################## CAMBIO FECHA DE NACIMIENTO POR EDAD ################################
                df["age"] = pd.to_datetime(df["age"], format="%Y/%m/%d")

                def datediff(x):
                    today = date.today()
                    age = today.year - x.year - ((today.month, today.day) < (x.month, x.day))
                    return age

                df["age"] = df["age"].apply(datediff)
                ###################################################################################################

                ################################# FILTRACION POR EDAD #############################
                df = df.loc[df["age"].between(18, 90)]
                ###################################################################################

                ########################### ELINACION DE PRE Y SUF ###########################
                        
                df["university"] = df["university"].str.lower()
                df["career"] = df["career"].str.lower()
                df["location"] = df["location"].str.lower()
                df["first_name"] = df["first_name"].str.lower()
                df["last_name"] = df["last_name"].str.lower()
                df["email"] = df["email"].str.lower()

                df = df.replace({'gender': {'F':'female','M':'male'}})
                
                df.career = df.career.str.strip()

                df = df.reindex(columns= ['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email'])

                df = df.astype({'inscription_date': 'string'})
                df = df.astype({'first_name': 'string'})
                df = df.astype({'last_name': 'string'})
                df = df.astype({'gender': 'string'})
                df = df.astype({'postal_code': 'string'})
                df = df.astype({'location': 'string'}) 
                
                df.to_csv("datasets/3AUFlores_process.txt", sep="\t", index=None)
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
                data = open("datasets/3AUFlores_process.txt", "rb")#####
                s3.Bucket("alkemy-p3").put_object(
                    Key="preprocess/3AUFlores_process.txt", Body=data
                )

                logging.info('Tarea de carga EXITOSA')
            except:
                logging.info('ERROR al cargar')

        Extraction() >> Transformation() >> Load()