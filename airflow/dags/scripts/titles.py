from sqlalchemy import create_engine
from datetime import datetime
from time import sleep
import requests
import gzip
import shutil
import pandas as pd
import numpy as np
import logging
import os


POSTGRES_HOST = 'host.docker.internal'  # os.environ['POSTGRES_HOST']
POSTGRES_USER = 'airflow'  # os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = 'airflow'  # os.environ['POSTGRES_PASSWORD']
POSTGRES_DB = 'postgres'    # os.environ['POSTGRES_DB']

# read from local folder
DATA_PATH = '/opt/airflow/data/'


def _remove_local_file(filename):
    counter = 0
    while 1:
        # try for 4 times only
        if counter > 3:
            return False
        else:
            counter += 1

        try:
            os.remove(os.path.join(DATA_PATH, filename))
            logging.info(f"Deleted file {filename}")
            return True
        except OSError:
            logging.error(f"Unable to delete file {filename}. File is still being used by another process.")
            sleep(2)


def upload_dim_title_desc():
    # read from tsv
    title_basics = pd.read_csv(os.path.join(DATA_PATH, 'title.basics.tsv'),
                               dtype={'tconst': 'str',
                                      'titleType': 'str',
                                      'primaryTitle': 'str',
                                      'originalTitle': 'str',
                                      'isAdult': 'bool',
                                      'startYear': 'Int64',
                                      'endYear': 'Int64',
                                      'runtimeMinutes': 'Int64', 'genres': 'str'},
                               sep='\t', na_values='\\N', quoting=3)

    title_basics[['genre_1', 'genre_2', 'genre_3']] = title_basics['genres'].str.split(',', expand=True)
    title_basics = title_basics.rename(columns={'titleType': 'type', 'primaryTitle': 'primary_title',
                                                'originalTitle': 'original_title',
                                                'isAdult': 'is_adult', 'startYear': 'start_year', 'endYear': 'end_year',
                                                'runtimeMinutes': 'runtime_minutes'})
    title_basics = title_basics.drop(['genres'], axis=1)

    title_ratings = pd.read_csv(os.path.join(DATA_PATH, 'title.ratings.tsv'), sep='\t', na_values='\\N')
    title_ratings = title_ratings.rename(columns={"averageRating": 'av_rating', 'numVotes': 'num_votes'})

    df_final = pd.merge(title_basics, title_ratings, on='tconst', how='inner')

    now = datetime.now()

    # insert df into dim_casts table
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}')
    sql = """SELECT id FROM download_date u WHERE u.month = %(mo)s AND u.day = %(da)s AND u.year = %(ye)s"""
    result = pd.read_sql(sql, con=engine, params={'mo': now.month, 'da': now.day, 'ye': now.year})
    df_final['download_date_id'] = result.values[0][0]

    df_final.to_sql('titles', engine, schema='public', if_exists="append", index=False)

    _remove_local_file('title.basics2.tsv')
    _remove_local_file('title.basics.tsv.gz')
    _remove_local_file('title.ratings2.tsv')
    _remove_local_file('title.ratings.tsv.gz')


