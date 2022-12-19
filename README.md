# IMDB_AIRFLOW
Simple application that builds a basic ETL pipeline that allows to extract, transform and load the data from IMDb

## Introduction
Project on ETL using Airflow, Postgresql and FastAPI. The IMDb datasets led to the following schema:
<p align="center">
  <img src="./images/schema.png" />
</p>

## Steps
1. Change the values in `airflow/config.json`
2. Initialize Airflow:
> docker-compose up airflow-init
3. Start all services using docker compose
> docker-compose up 
4. Visit `Airflow Web UI` at `localhost:8080` and login using
> username: airflow
>
> password: airflow
5. Trigger the `download_imdb_datasets` DAG and both  DAGS: `create_tables` and  `etl_imdb` will be triggered after the first one has finished. 
8. Visit `FastAPI` at `http://127.0.0.1:8000/docs` and choose a category to return the list

