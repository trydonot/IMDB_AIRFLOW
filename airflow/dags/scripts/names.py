from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os

pd.options.mode.chained_assignment = None  # default='warn'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

POSTGRES_HOST = 'host.docker.internal'  # os.environ['POSTGRES_HOST']
POSTGRES_USER = 'airflow'  # os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = 'airflow'  # os.environ['POSTGRES_PASSWORD']
POSTGRES_DB = 'postgres'  # os.environ['POSTGRES_DB']

# read from local folder
DATA_PATH = '/opt/airflow/data/'


def upload_dim_names():
    # read from tsv
    title_principals = pd.read_csv(os.path.join(DATA_PATH, 'title.principals.tsv'), sep='\t', na_values='\\N')
    title_principals_df = title_principals.drop(['ordering', 'job'], axis=1)

    name_basics = pd.read_csv(os.path.join(DATA_PATH, 'name.basics.tsv'),
                              dtype={'nconst': 'str',
                                     'primaryName': 'str',
                                     'birthYear': 'Int64',
                                     'deathYear': 'Int64',
                                     'primaryProfession': 'str',
                                     'knownForTitles': 'str'},
                              sep='\t', na_values='\\N')
    name_basics = name_basics.rename(
        columns={'primaryName': 'name', 'birthYear': 'birth_year', 'deathYear': 'death_year'})
    # drop unused columns
    name_basics_df_dropped = name_basics.drop(['primaryProfession', 'knownForTitles'], axis=1)
    df_casts = pd.merge(title_principals_df, name_basics_df_dropped, on='nconst', how='inner')

    df_upload = df_casts.drop(['tconst', 'characters', 'category'], axis=1)
    df_upload = df_upload.drop_duplicates(subset=['nconst'])
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}')

    # insert df into dim_names table
    print(df_upload)
    df_upload.to_sql('names', engine, if_exists="append", index=False)

    sql = """SELECT id, tconst FROM titles"""           # select id and title_id
    titles_id_df = pd.read_sql(sql, con=engine)

    sql2 = """SELECT id, nconst FROM names"""           # select id and name_id
    casts_id_df = pd.read_sql(sql2, con=engine)

    df_casts_composite_table = df_casts[['nconst', 'tconst', 'category']]
    df_tmp = pd.merge(casts_id_df, df_casts_composite_table, on='nconst', how='inner')
    df_titles_casts = pd.merge(df_tmp, titles_id_df, on='tconst', how='inner')
    df_titles_casts = df_titles_casts.drop(['tconst', 'nconst'], axis=1)
    df_titles_casts = df_titles_casts.rename(columns={'id_x': 'name_id', 'id_y': 'title_id'})

    # insert df into titles_names table
    df_titles_casts.to_sql('titles_names', engine, if_exists="append", index=False)


def main():
    upload_dim_names()


if __name__ == '__main__':
    main()
