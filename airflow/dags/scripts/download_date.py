from sqlalchemy import create_engine
from datetime import date, datetime
import pandas as pd
import os

POSTGRES_HOST = 'host.docker.internal'  # os.environ['POSTGRES_HOST']
POSTGRES_USER = 'airflow'   # os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = 'airflow'   # os.environ['POSTGRES_PASSWORD']
POSTGRES_DB = 'postgres'      # os.environ['POSTGRES_DB']


def upload_download_date():
    # create a df with year, month, day with today's date
    now = datetime.now()

    data = [(now.year, now.month, now.day)]
    date_df = pd.DataFrame(data, columns=['year', 'month', 'day'])

    # insert dataset download date into db

    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}')
    date_df.to_sql('download_date', engine, if_exists="append", index=False)


def main():
    upload_download_date()


if __name__ == '__main__':
    main()
