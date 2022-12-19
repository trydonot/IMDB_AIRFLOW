from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow import settings
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from time import sleep
import os
import logging
import datetime
import requests
import gzip
import shutil


def create_conn(conn_id, conn_type, host, login=None, password=None, port=None, schema=None, extra=None):
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        port=port,
        schema=schema,
        password=password,
        login=login,
        extra=extra
    )
    session = settings.Session()
    conn_name = session \
        .query(Connection) \
        .filter(Connection.conn_id == conn.conn_id) \
        .first()

    if str(conn_name) == str(conn_id):
        return logging.info(f"Connection {conn_id} already exists")

    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {conn_id} is created')


with DAG(
        dag_id="create_tables",
        start_date=days_ago(0),
        schedule_interval= None,
        catchup=False
) as dag:
    create_postgres_cnx = PythonOperator(
        task_id="create_postgres_cnx",
        python_callable=create_conn,
        op_kwargs={
            "conn_id": "imdb_airflow_postgres",
            "conn_type": "postgres",
            "host": "host.docker.internal",
            "login": "airflow",
            "password": "airflow",
            "schema": "postgres",
            "port": 5432,
        },
    )

    create_download_date_table = PostgresOperator(
        task_id="create_download_date_table",
        postgres_conn_id="imdb_airflow_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS download_date (
            id SERIAL PRIMARY KEY,
            year SMALLINT NOT NULL,
            month SMALLINT NOT NULL,
            day SMALLINT NOT NULL
        );
        """,
    )

    create_episodes_table = PostgresOperator(
        task_id="create_episodes_table",
        postgres_conn_id="imdb_airflow_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS episodes (
            id SERIAL PRIMARY KEY,
            title_id INT REFERENCES titles (id),
            season SMALLINT,
            total_episodes SMALLINT
        );
        """,
    )

    create_titles_table = PostgresOperator(
        task_id="create_titles_table",
        postgres_conn_id="imdb_airflow_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS titles (
            id SERIAL PRIMARY KEY,
            download_date_id INT,
            tconst VARCHAR(20) UNIQUE NOT NULL,
            type VARCHAR(40),
            primary_title VARCHAR(800) NOT NULL,
            original_title VARCHAR(800),
            is_adult BOOLEAN,
            start_year SMALLINT,
            end_year SMALLINT,
            runtime_minutes SMALLINT,
            av_rating FLOAT(2),
            num_votes INT,
            genre_1 VARCHAR(25),
            genre_2 VARCHAR(25),
            genre_3 VARCHAR(25)
        );
        """,
    )

    create_names_table = PostgresOperator(
        task_id="create_names_table",
        postgres_conn_id="imdb_airflow_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS names (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            birth_year SMALLINT,
            death_year SMALLINT,
            nconst VARCHAR(15) NOT NULL
        );
        """,
    )

    create_titles_names_table = PostgresOperator(
        task_id="create_titles_names_table",
        postgres_conn_id="imdb_airflow_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS titles_names (
            title_id INT REFERENCES titles (id),
            name_id INT REFERENCES names (id),
            category VARCHAR(300)
        );
        """,
    )

    set_fkey = PostgresOperator(
        task_id="set_fkey",
        postgres_conn_id="imdb_airflow_postgres",
        sql="""
        ALTER TABLE titles
        ADD CONSTRAINT titles_download_date_id_fkey FOREIGN KEY (download_date_id)
        REFERENCES download_date (id);
        """,
    )

    drop_schema = PostgresOperator(
        task_id="drop_schema",
        postgres_conn_id="imdb_airflow_postgres",
        sql="""
        DROP SCHEMA IF EXISTS public CASCADE;
        """,
    )

    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="imdb_airflow_postgres",
        sql="""
        CREATE SCHEMA IF NOT EXISTS public;
        """,
    )

    trigger_first_load = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        trigger_dag_id="etl_imdb",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"message": "Hello World"},
    )

    create_postgres_cnx >> drop_schema >> create_schema
    create_schema >> create_titles_table >> set_fkey
    create_schema >> create_download_date_table >> set_fkey
    create_titles_table >> create_episodes_table
    create_titles_table >> create_names_table >> create_titles_names_table >> trigger_first_load
