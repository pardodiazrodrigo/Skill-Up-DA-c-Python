import os
import logging
import csv
import boto3
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task

default_args = {
    "owner": "P3",
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

# Instantiate DAG
with DAG(
    dag_id="3A_3AUNFlores_dag_etl_ETL",
    start_date=datetime(2022, 11, 3),
    max_active_runs=5,
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
) as dag:

    with open('include/SQL_3AUFlores.sql', 'r') as myfile:
        data = myfile.read()
        print(data)
    
    @task()
    def Extraccion(**kwargs):
        
        #################################### OBTENCION TABLA BASE DE DATOS ################################
        try:
            hook = PostgresHook(postgres_conn_id="alkemy_db")
            df = hook.get_pandas_df(sql=data)
            df.to_csv("files/3AUFlores_select.csv")#####
            logging.info('Tarea de extraccion EXITOSA')
        except:
            logging.info('ERROR al extraer')
        ###################################################################################################
       
    @task()
    def Transformacion(**kwargd):
        
        try:
            
            dfSel = pd.read_csv('files/3AUFlores_select.csv', sep=',')#####

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

            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("MR._",""))##
            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("DR._",""))##
            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("MRS._",""))##

            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" MD",""))
            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" DDS",""))
            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" PHD",""))
            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" DVM",""))
            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" JR.",""))
            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" II",""))
            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace(" IV",""))
            
            dfSel["last_name"] = dfSel["last_name"].apply(lambda x: x.replace("_"," "))##

            dfSel['first_name'] = dfSel['last_name'].map(lambda x:x.split(' ') [0])
            dfSel['last_name'] = dfSel['last_name'].map(lambda x:x.split(' ') [1])
            ###################################################################################################

            ########################## COMPLETAMINETO CAMPO POSTAL_CODE O LOCATION ############################         
            dfCod = pd.read_csv('assets/codigos_postales.csv',sep=',')
            dfCod = dfCod.drop_duplicates(['localidad'], keep='last')
            
            dfC = dfSel['postal_code']
            dfL = dfSel['location']
            dfC = dfC.dropna()
            dfL = dfL.dropna()
            TamC = dfC.size
            TamL = dfL.size

            if TamL == 0:
                df = pd.merge(dfSel,dfCod,left_on='postal_code',right_on='codigo_postal')
                del df['codigo_postal']
                del df['location']
                del df['Unnamed: 0']
                df = df.rename(columns={'localidad':'location'})

            if TamC == 0:
                df = pd.merge(dfSel,dfCod,left_on='location',right_on='localidad')
                del df['localidad']
                del df['postal_code']
                del df['Unnamed: 0']
                df = df.rename(columns={'codigo_postal':'postal_code'})

            ###################################################################################################
            #df.to_csv("/usr/local/airflow/include/3CUFlores_select_final.csv")
            ############################## CAMBIO FECHA DE NACIMIENTO POR EDAD ################################
            first_value = df['age'].values[0]

            date_formats = ['%d/%b/%y', '%Y/%m/%d', '%d-%b-%y', '%Y-%m-%d']
            for date_format in date_formats:
                try:
                    d = datetime.strptime(first_value, date_format)
                    print(date_format)
                    break
                except ValueError as e:
                    pass

            df["age"] = pd.to_datetime(df["age"], format=date_format)

            def datediff(x):
                today = date.today()
                age = today.year - x.year - ((today.month, today.day) < (x.month, x.day))
                return age

            df["age"] = df["age"].apply(datediff)
            ###################################################################################################

            ################################# FILTRACION POR EDAD #############################
            df = df.loc[df["age"].between(18, 90)]
            ###################################################################################

            ################################# NORMALIZACION ###################################
            try:        
                df["inscription_date"] = pd.to_datetime(df["inscription_date"], format="%d-%b-%y")
            except:
                pass

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

            df.to_csv("datasets/3AUFlores_process.txt", sep="\t", index=None)#####

            logging.info('Tarea de transformacion EXITOSA')
        except:
            logging.info('ERROR al transformar')

    @task()
    def Carga(**kwargd):

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

    Extraccion() >> Transformacion() >> Carga()