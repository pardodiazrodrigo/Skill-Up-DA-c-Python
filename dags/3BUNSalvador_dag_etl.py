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
        with open(
            "/home/nvrancovich/airflow/dags/Skill-Up-DA-c-Python/datasets/3BUNSalvador_select.csv", "r", encoding="utf-8"
        ) as file:
            df = pd.read_csv(file, index_col=[0])

        cp = pd.read_csv('/home/nvrancovich/airflow/dags/Skill-Up-DA-c-Python/assets/codigos_postales.csv')

        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], format="%Y-%m-%d")

        def age(x):
            today = date.today()
            age = today.year - x.year - ((today.month, today.day) < (x.month, x.day))
            return age

        df["age"] = df["date_of_birth"].apply(age)
        df = df.loc[df["age"].between(18, 90)]

        cp.rename(columns={"codigo_postal": "postal_code","localidad": "location",}, 
                inplace=True)

        df.drop(columns="location",
                inplace=True)

        df = df.merge(cp, on="postal_code", how="left")

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
            " iv",
        ]

        for word in words:
            df.full_name = df.full_name.replace(word, '')

        df[["first_name", "last_name"]] = df.full_name.str.split("_", n=1, expand=True)

        df.drop(columns="full_name",
                inplace=True)

        df.gender = df.gender.replace('F', 'female')
        df.gender = df.gender.replace('M', 'male')

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

        df.to_csv("/home/nvrancovich/airflow/dags/Skill-Up-DA-c-Python/3BUNComahue_process.txt", sep="\t", index=None)

    @task
    def salvador_load():
        pass

    salvador_extract() >> salvador_transform() >> salvador_load()