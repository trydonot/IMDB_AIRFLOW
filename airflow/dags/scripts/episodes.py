from sqlalchemy import create_engine
from datetime import datetime
from time import sleep
import requests
import gzip
import shutil
import datetime
import pandas as pd
import numpy as np
import logging
import os

pd.options.mode.chained_assignment = None  # default='warn'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

POSTGRES_HOST = 'host.docker.internal'  # os.environ['POSTGRES_HOST']
POSTGRES_USER = 'airflow'  # os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = 'airflow'  # os.environ['POSTGRES_PASSWORD']
POSTGRES_DB = 'postgres'    # os.environ['POSTGRES_DB']

# read from local folder
DATA_PATH = '/opt/airflow/data/'


def _remove_local_file(filename):
    counter = 0
    while 1:
        # try for 5 times only
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


def upload_dim_episodes():
    # read from tsv
    title_episode_df = pd.read_csv(os.path.join(DATA_PATH, 'title.episode.tsv'),
                                   dtype={'tconst': 'str',
                                          'parentTconst': 'str',
                                          'seasonNumber': 'Int64',
                                          'episodeNumber': 'Int64'},
                                   sep='\t',
                                   na_values='\\N')

    title_episodes_df = title_episode_df.drop(columns=['tconst'])

    null_df = title_episodes_df[title_episodes_df["seasonNumber"].isna() & title_episodes_df["episodeNumber"].isna()]
    null_df['total_episodes'] = null_df.groupby(['parentTconst'])['parentTconst'].transform('count')
    null_df['seasonNumber'] = None
    null_df = null_df.rename(columns={'seasonNumber': 'season', 'parentTconst': 'tconst'})

    notnull_df = title_episodes_df[
        title_episodes_df["seasonNumber"].notna() & title_episodes_df["episodeNumber"].notna()]
    notnull_df['total_episodes'] = notnull_df.groupby(['parentTconst', 'seasonNumber'])['episodeNumber'].transform(
        'max')
    notnull_df = notnull_df.rename(columns={'seasonNumber': 'season', 'parentTconst': 'tconst'})

    df_final = pd.concat([null_df, notnull_df])

    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}')
    sql = """SELECT tconst, id FROM titles"""
    titles_pg_df = pd.read_sql(sql, con=engine)

    df_final = pd.merge(df_final, titles_pg_df, on=['tconst'])
    df_final = df_final.drop(columns=['tconst', 'episodeNumber'])
    df_final = df_final.rename(columns={'id': 'title_id'})

    # insert df into table
    df_final.to_sql('episodes', engine, schema='public', if_exists="append", index=False)
    _remove_local_file('title.episode2.tsv')
    _remove_local_file('title.episode.tsv.gz')


def main():
    upload_dim_episodes()


if __name__ == '__main__':
    main()
