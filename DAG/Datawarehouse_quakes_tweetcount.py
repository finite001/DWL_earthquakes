import datetime
import psycopg2
import psycopg2.extras
import requests
import time
import pandas as pd
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def conn_postgres():
    pg_hook = PostgresHook(postgres_conn_id="rds", schema="dbeq")
    connection = pg_hook.get_conn()
    connection.set_session(autocommit=True)
    cursor = connection.cursor()
    return pg_hook, connection, cursor


def start():
    logging.info('Starting the DAG_')


def adjust_and_merge():
    db_hook = PostgresHook(postgres_conn_id="rds", schema="dbeq")
    earthquakes_df = db_hook.get_pandas_df("SELECT * FROM quakes")
    #earthquakes_df = pd.DataFrame(earthquakes_df)
    logging.info("established connection")
    #pg_hook, connection, cursor = conn_postgres()
    #earthquakes = pd.read_sql("SELECT * from quakes", connection)

    #create new column and change to date typ
    earthquakes_df["date"] = earthquakes_df["time"]
    # Change datatype from miliseconds to datetime (column "time") and from miliseconds to date (column "Date")
    count = 0
    for i in earthquakes_df.iloc[:, 8]:
        earthquakes_df.iloc[count, 8] = datetime.datetime.fromtimestamp(earthquakes_df.iloc[count, 8] / 1000.0)
        count += 1
    earthquakes_df["time"] = pd.to_datetime(earthquakes_df['date'])
    earthquakes_df["date"] = pd.to_datetime(earthquakes_df['date']).dt.date
    #logging.info(f'Successfully used PostgresHook to return {earthquakes_df.columns} records')
    #logging.info(f'{earthquakes_df["date"]}')

    ##Access twitter data and create df
    twitter_df = db_hook.get_pandas_df("SELECT * FROM tweetcount")
    #Adjust twitter table
    #change type of "starttime"
    twitter_df["starttime"] = pd.to_datetime(twitter_df["starttime"]).dt.date
    # Rename column "starttime"
    twitter_df.rename(columns={"starttime": "date"}, inplace=True)
    # Drop Endtime
    twitter_df.drop("endtime", axis=1, inplace=True)

    logging.info(f'{twitter_df.head(2)} records_twitter')
    logging.info(f'{earthquakes_df.head(2)} records_quakes')

    # merge tables
    earthquake_tweetcount = earthquakes_df.merge(twitter_df, on='date', how='left')

    logging.info(f'{earthquake_tweetcount.head(10)} records_quakes')
    logging.info(f'{earthquake_tweetcount.columns} records_quakes')

    # write data to RDS table
    # Now set up insert statement
    sql = 'INSERT INTO datawarehouse_earthquake_tweetcount(id, magnitude, time, type, title, coord_lat, coord_long, date, count) VALUES '
    for index, row in earthquake_tweetcount.iterrows():
        try:
            sql += ("('{}',{},'{}','{}','{}',{},{},'{}',{}), ".format(row['id'],row['magnitude'],row['time'],row['type'],row['title'],row['coord_lat'],row['coord_long'],row['date'],row['count']))
        except:
            logging.info("Problem with earthquake ({}, {})".format(row['id'],row['title']))


dag = DAG(
        'earthquake_tweetcount_datawarehouse',
        schedule_interval='@daily',
        start_date=datetime.datetime.now() - datetime.timedelta(days=30))


startup_dag = PythonOperator(
   task_id="startup_dag",
   python_callable=start,
   dag=dag
)


create_datawarehouse_table = PostgresOperator(
    task_id="create_datawarehouse_table",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
        CREATE TABLE IF NOT EXISTS datawarehouse_earthquake_tweetcount (
        id varchar(255),
        magnitude real,
        time bigint,
        type varchar(255),
        title varchar(255),
        coord_lat real,
        coord_long real,
        date date,
        count int
        );
        '''
)

adjust_and_merge = PythonOperator(
    task_id='adjust_and_merge',
    dag=dag,
    python_callable=adjust_and_merge
)


startup_dag >> create_datawarehouse_table >> adjust_and_merge
