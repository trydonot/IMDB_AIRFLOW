from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from time import sleep
import os
import logging
import requests
import gzip
import shutil
import datetime

IMDB_DATASETS_BASE_URL = "https://datasets.imdbws.com/"


def _unzip_gz(file):
    with gzip.open(file, 'rb') as f_in:  # unzip and open the .gz file
        filename = file.split('.gz')[0]
        with open(filename, 'wb') as f_out:  # open another blank file
            shutil.copyfileobj(f_in, f_out)  # copy the .gz file contents to the blank file
    logging.info(f"Finished unzipping {file}. Output is {filename}")

    return filename


def download_file(base_url, filename):
    # concatenate to get the url of the file
    full_path = base_url + filename

    logging.info(f"Downloading from {full_path}")
    gz_filename = full_path.split('/')[-1]

    path = '/opt/airflow/data/'

    if os.path.isdir(path):
        os.chdir(path)
    else:
        os.mkdir(path)
        os.chdir(path)

    with requests.get(full_path, stream=True) as r:
        r.raise_for_status()
        gz_filename_path = path + gz_filename
        with open(gz_filename_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=100):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                # if chunk:
                f.write(chunk)
    logging.info(f"Finished downloading from {full_path}")

    tsv_file = _unzip_gz(gz_filename_path)


def init_download():
    now = datetime.datetime.now()
    logging.info(f"Started downloading at {now}")


with DAG(
        dag_id='download_imdb_datasets',
        schedule_interval='@once',
        start_date=days_ago(0),
        max_active_runs=1,
) as dag:
    download_name_basics = PythonOperator(
        task_id="download_name_basics",
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL,
            "filename": "name.basics.tsv.gz",
        },
    )

    download_title_akas = PythonOperator(
        task_id="download_title_akas",
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL,
            "filename": "title.akas.tsv.gz",
        },
    )

    download_title_basics = PythonOperator(
        task_id="download_title_basics",
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL,
            "filename": "title.basics.tsv.gz",
        },
    )

    download_title_crew = PythonOperator(
        task_id="download_title_crew",
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL,
            "filename": "title.crew.tsv.gz",
        },
    )

    download_title_episode = PythonOperator(
        task_id="download_title_episode",
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL,
            "filename": "title.episode.tsv.gz",
        },
    )

    download_title_principals = PythonOperator(
        task_id="download_title_principals",
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL,
            "filename": "title.principals.tsv.gz",
        },
    )

    download_title_ratings = PythonOperator(
        task_id="download_title_ratings",
        python_callable=download_file,
        op_kwargs={
            'base_url': IMDB_DATASETS_BASE_URL,
            "filename": "title.ratings.tsv.gz",
        },
    )
    init_download = PythonOperator(
        task_id="init_download",
        python_callable=init_download
    )

    trigger_create_db = TriggerDagRunOperator(
        task_id="trigger_create_db",
        trigger_dag_id="create_tables",  # Ensure this equals the dag_id of the DAG to trigger
        wait_for_completion=True,
        conf={"message": "Hello World"},
    )

    init_download >> [download_name_basics, download_title_basics, download_title_crew, download_title_episode,
                      download_title_principals, download_title_ratings] >> trigger_create_db
