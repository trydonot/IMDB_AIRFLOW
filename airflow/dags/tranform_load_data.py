import os
import sys
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
sys.path.append('/opt/airflow/dags/scripts/')
import download_date
import episodes
import names
import titles


with DAG(
        dag_id='etl_imdb',
        schedule_interval=None,
        start_date=days_ago(0),
        catchup=False
) as dag:

    titles = PythonOperator(
        task_id="titles",
        python_callable=titles.upload_dim_title_desc,
    )

    download_date = PythonOperator(
        task_id="download_date",
        python_callable=download_date.upload_download_date,
    )

    episodes = PythonOperator(
        task_id="episodes",
        python_callable=episodes.upload_dim_episodes,
    )

    names = PythonOperator(
        task_id="names",
        python_callable=names.upload_dim_names,
    )

    download_date >> titles >> episodes >> names
