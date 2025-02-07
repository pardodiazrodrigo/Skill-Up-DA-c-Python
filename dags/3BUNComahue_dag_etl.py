import os
import pandas as pd
from datetime import date, datetime, timedelta
import logging
import boto3

from airflow.models import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

DIR = os.path.dirname(os.path.abspath(__file__))
DIR = os.path.abspath(os.path.join(DIR, os.pardir))

default_args = {
    "owner": "P3",
    "retries": 5,
    "retry_delay": timedelta(minutes=60),
}

with DAG(
    dag_id="3BUNComahue_dag_etl",
    start_date=datetime(2022, 11, 3),
    max_active_runs=5,
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    @task()
    def extract():
        logging.info('extract process started')


        try:
            # Query
            with open(f'{DIR}/include/3BUNComahue.sql','r',encoding='utf-8') as f:
                sql_script = f.read()
            hook = PostgresHook(postgres_conn_id="alkemy_db")
            df = hook.get_pandas_df(sql=sql_script)
            df.to_csv(f"{DIR}/files/3BUNComahue_select.csv")
            logging.info("csv file created")

        except Exception as e:
            logging.error(e)


    @task()
    def transform():
        logging.info('transform process started')

        try:
            with open(f'{DIR}/assets/codigos_postales.csv','r',encoding='utf-8') as f:
                cod_post_df = pd.read_csv(f)

            with open(f"{DIR}/files/3BUNComahue_select.csv") as f:
                df = pd.read_csv(f,index_col=[0])

            if df['postal_code'].isnull().values.any():
                df['location'] = df['location'].astype(str)
                df.location = df.location.str.replace('_', ' ')

                cod_post_df.rename(columns={"codigo_postal": "postal_code","localidad": "location",}, 
                inplace=True)

                df.drop(columns="postal_code",
                inplace=True)

                df = df.merge(cod_post_df, on="location", how="left")

            if df['location'].isnull().values.any():
                cod_post_df.rename(columns={"codigo_postal": "postal_code","localidad": "location",}, 
                inplace=True)

                df.drop(columns="location",
                inplace=True)

                df = df.merge(cod_post_df, on="postal_code", how="left")

            gender = {
                'f':'female',
                'm':'male',
                'F':'female',
                'M':'male'
            }

            df = df.replace({'gender': gender})

            df.full_name = df.full_name.astype(str)

            words = [
            "mr. ",
            "mrs. ",
            "miss ",
            "dr. ",
            " md",
            " dds",
            " phd",
            " dvm",
            " jr.",
            " ii",
            " iv"]

            for word in words:
                df.full_name = df.full_name.replace(word, '')

            try:
                df[['first_name','last_name']] = df.full_name.str.split(" ",n=1,expand=True)
            except:
                pass
            try:
                df[['first_name','last_name']] = df.full_name.str.split("_",n=1,expand=True)
            except:
                pass

            df = df.drop(['full_name'], axis=1)

            df.career = df.career.str.strip()
            df.career = df.career.str.lower()

            def age(born):
                born = datetime.strptime(born, "%Y-%m-%d").date()
                today = date.today()
                age =  today.year - born.year - ((today.month, today.day) < (born.month, born.day))
                return age

            df['age'] = df['date_of_birth'].apply(age)
            df = df.drop(['date_of_birth'], axis=1)

            df = df.loc[df["age"].between(18, 90)]

            df.university = df.university.astype(str)
            df.career = df.career.astype(str)
            df.first_name = df.first_name.astype(str)
            df.last_name = df.last_name.astype(str)
            df.location = df.location.astype(str)
            df.email = df.email.astype(str)

            df.university = df.university.str.replace('_', ' ')
            df.career = df.career.str.replace('_', ' ')

            df.university = df.university.str.lower()
            df.career = df.career.str.lower()
            df.first_name = df.first_name.str.lower()
            df.last_name = df.last_name.str.lower()
            df.location = df.location.str.lower()
            df.email = df.email.str.lower()

            df = df.reindex(columns=[
                        "university",
                        "career",
                        "inscription_date",
                        "first_name",
                        "last_name",
                        "gender",
                        "age",
                        "postal_code",
                        "location", 
                        "email"])

            df.to_csv(f"{DIR}/datasets/3BUNComahue_process.txt",sep="\t",index=None)

            logging.info('txt file succesfully created')
        except Exception as e:
            logging.error(e)

    @task()
    def load():
        logging.info('loading process started')
        ACCESS_KEY = "AKIASMO2Y7NKXVBNK7QN"
        SECRET_ACCESS_KEY = "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"
        try:
            session = boto3.Session(
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_ACCESS_KEY,
            )
            s3 = session.resource("s3")
            data = open(f"{DIR}/datasets/3BUNComahue_process.txt", "rb")
            s3.Bucket("bucket-alk").put_object(
                Key="preprocess/3BUNComahue_process.txt", Body=data
            )

            logging.info('txt file loading succesfully done')

        except Exception as e:
            logging.error(e)

    extract() >> transform() >> load()