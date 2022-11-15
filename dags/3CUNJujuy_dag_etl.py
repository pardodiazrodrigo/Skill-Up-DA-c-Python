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

# Default settings applied to all tasks
default_args = {
    "owner": "P3",
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

# Instantiate DAG
with DAG(
    dag_id="3C_UNJujuy_ETL",
    start_date=datetime(2022, 11, 3),
    max_active_runs=5,
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
) as dag:

    @task()
    def extract():
        #################################### 1. EXTRACT #######################################################
        logging.info("Inicio de extraccion!")
        try:
            with open(
                "include/P3_UniJujuy.sql",
                "r",
                encoding="utf-8",
            ) as my_file:
                query = my_file.read()
            hook = PostgresHook(postgres_conn_id="alkemy_db")
            df = hook.get_pandas_df(sql=query)
            df.to_csv("files/3CUJujuy_select.csv")
            logging.info("Extraccion exitosa!")
        except Exception as e:
            logging.exception("Exception occurred", exc_info=True)
        #######################################################################################################

    @task()
    def transform():
        #################################### 2. TRANSFORM #####################################################
        logging.info("Inicio de la transformacion!")
        try:
            with open("files/3CUJujuy_select.csv", "r", encoding="utf-8") as my_file:
                df = pd.read_csv(my_file, index_col=[0])
            
            first_value = df['age'].values[0]

            date_formats = ["%d/%b/%y", "%Y/%m/%d"]
            for date_format in date_formats:
                try:
                    datetime.strptime(first_value, date_format)
                    print(date_format)
                    break
                except Exception as e:
                    logging.exception("Exception occurred", exc_info=True)
                
            df["age"] = pd.to_datetime(df["age"], format=date_format)

            def datediff(x):
                today = date.today()
                age = (
                    today.year - x.year - ((today.month, today.day) < (x.month, x.day))
                )
                return age

            df["age"] = df["age"].apply(datediff)

            ###################################################################################################

            #################################### ELIMINACION DE MENORES DE 18 Y MAYORES DE 90 #################
            df = df.loc[df["age"].between(18, 90)]
            ###################################################################################################
            
            #################################### COMPLETA CAMPO LOCATION ######################################
            with open(
                "assets/codigos_postales.csv",
                "r",
                encoding="utf-8",
            ) as my_file:
                dfCod = pd.read_csv(my_file)
            dfCod = dfCod.drop_duplicates(["localidad"], keep="first")
            dfCod[["codigo_postal", "localidad"]] = dfCod[
                ["codigo_postal", "localidad"]
            ].astype("string")
            dfCod.rename(
                columns={
                    "codigo_postal": "postal_code",
                    "localidad": "location",
                },
                inplace=True,
            )
            dfCod["location"] = dfCod["location"].str.lower()
           
            if df['postal_code'].isnull().values.any():
                df.drop(
                     columns="postal_code",
                     inplace=True,
                )
                df["location"] = df["location"].astype("string")
                df = df.merge(dfCod, on="location", how="left")

            if df['location'].isnull().values.any():
                df.drop(
                   columns=["location"],
                     inplace=True,
                )
                df["postal_code"] = df["postal_code"].astype("string")
                df = df.merge(dfCod, on="postal_code", how="left")

            ###################################################################################################
            
            #################################### NORMALIZACION Y DIVISION DE CAMPO LAST_NAME ##################
            df["last_name"] = df["last_name"].astype("string")
            df['last_name'] = df['last_name'].str.lower()
            df.last_name = df.last_name.str.replace("_", " ", regex=True)
            prefixs = [
                "mr. ",
                "mrs. ",
                "miss ",
                "dr. ",
            ]
            for prefix in prefixs:
                df.last_name = df.last_name.str.removeprefix(prefix)
            sufixs = [
                " md",
                " dds",
                " phd",
                " dvm",
                " jr.",
                " ii",
                " iv",
            ]
            for sufix in sufixs:
                df.last_name = df.last_name.str.removesuffix(sufix)

            df[["first_name", "last_name"]] = df.last_name.str.split(
                " ", n=1, expand=True
            )
            ###################################################################################################
            
            #################################### NORMALIZACION FINAL ##########################################
            df.university = df.university.str.replace("_", " ", regex=True)
            df.university = df.university.str.strip()
            df.career = df.career.str.replace("_", " ", regex=True)
            df.career = df.career.str.strip()
            df["gender"] = df["gender"].str.lower()
            df = df.replace({"gender": {"f": "female", "m": "male"}})
            df = df.reindex(
                columns=[
                    "university",
                    "career",
                    "inscription_date",
                    "first_name",
                    "last_name",
                    "gender",
                    "age",
                    "postal_code",
                    "location",
                    "email",
                ]
            )
            df[
                [
                    "university",
                    "career",
                    "inscription_date",
                    "gender",
                    "postal_code",
                    "location",
                    "email",
                ]
            ] = df[
                [
                    "university",
                    "career",
                    "inscription_date",
                    "gender",
                    "postal_code",
                    "location",
                    "email",
                ]
            ].astype(
                "string"
            )
            ###################################################################################################

            #################################### EXPORTACION CSV ##############################################
            df.to_csv("datasets/3CUJujuy_process.csv")
            df.to_csv(
                "datasets/3CUJujuy_process.txt",
                index=None,
            )
            logging.info("Transformacion exitosa!")
        except Exception as e:
            logging.exception("Exception occurred", exc_info=True)

    @task()
    def load():
        logging.info("Inicio de la carga en bucket S3!")
        try:
            ACCESS_KEY = "AKIAY27PJEHOPCMGIA7C"
            SECRET_ACCESS_KEY = "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"
            session = boto3.Session(
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_ACCESS_KEY,
            )
            s3 = session.resource("s3")
            data = open("datasets/3CUJujuy_process.txt", "rb")
            s3.Bucket("alkemy-p3").put_object(
                Key="preprocess/3CUJujuy_process.txt", Body=data
            )
            logging.info("Carga exitosa!")
            logging.info("ETL exitoso!! --> DAG finalizado!")
        except Exception as e:
            logging.exception("Exception occurred", exc_info=True)

    extract() >> transform() >> load()