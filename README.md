# IMDB_AIRFLOW
Simple application that builds a basic ETL pipeline that allows to extract, transform and load the data from IMDb

## Introduction
Project on ETL using Airflow, Postgresql and FastAPI.

## Steps
1. Initialize Airflow:
> docker-compose up airflow-init
2. Start all services using docker compose
> docker-compose up 
3. Visit `Airflow Web UI` at `localhost:8080` and login using
> username: airflow
>
> password: airflow
4. Trigger the `download_imdb_datasets` DAG and both  DAGS: `create_tables` and  `etl_imdb` will be triggered after the first one has finished. All DAGS are scheduled to run daily, ideally only etl_idb would be needed, but due to time constrains the hash column for comparing if data already existed was not implemented. Due to this, the table is being dropped and all data is being loaded daily.
5. Visit `FastAPI` at `http://127.0.0.1:8000/docs` and write a category (director,actress,actor) to return the list

