import datetime
import logging
import pandas as pd
from pandas import json_normalize
import requests
import json
import psycopg2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

# Helper function to split the coordinates into seperate columns
def splitCoords(coords, ax):
    coords = list(coords)
    
    if ax == 'lat':
        return coords[0]
    elif ax == 'long':
        return coords[1]
    else:
        return coords[2]

# Load all earthquakes from the API and set up insert statement
def load_earthquakes(ti):
    logging.info('Starting to load the earthquakes from API...')
    starttime = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    endtime = (datetime.datetime.now()).strftime("%Y-%m-%d")
    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime='+starttime+'&endtime='+endtime+'&minmagnitude=5' # concatenate request
    
    r = requests.get(url) # request the API

    r_json = json.loads(r.text)["features"] # turn the result into a json and extract all "Features" 
    df = json_normalize(r_json) # turn it into a pandas dataframe

    logging.info(df)
    df['coord.lat'] = df.apply(lambda row: splitCoords(row['geometry.coordinates'],'lat'),axis=1)
    df['coord.long'] = df.apply(lambda row: splitCoords(row['geometry.coordinates'],'long'),axis=1)
    df['coord.height'] = df.apply(lambda row: splitCoords(row['geometry.coordinates'],'height'),axis=1)

    # Now set up insert statement
    problemquakes = []
    sql = 'INSERT INTO quakes(id, magnitude, time, type, title, coord_lat, coord_long, coord_height) VALUES '
    for index, row in df.iterrows():
        try:
            sql += ("('{}',{},{},'{}','{}',{},{},{}), ".format(row['id'],row['properties.mag'],row['properties.time'],row['properties.type'],row['properties.title'],row['coord.lat'],row['coord.long'],row['coord.height']))
        except: 
            problemquakes.append(row['id']) # Save all the earthquakes that failed to fill into the DB in this list
            logging.info("Problem with earthquake ({}, {})".format(row['id'],row['properties.title']))
            
    sql = sql[:-2] # remove last , to separate values        
    sql += ';' # add final ;
    ti.xcom_push(key='earthquakes', value=sql) # set to memory xcom

def save_earthquakes(ti):
    earthquakes_sql = ti.xcom_pull(key='earthquakes', task_ids='load_earthquakes') # get from memory xcom

    # connection to postgresql db
    pg_hook = PostgresHook(postgres_conn_id="datalake", schema="earthquake")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    connection.set_session(autocommit=True)

    # insert earthquakes into DB
    cursor.execute(earthquakes_sql)


# Set up DAG
dag = DAG(
        'earthquake',
        schedule_interval='@daily',
        start_date=datetime.datetime.now() - datetime.timedelta(days=1))

load_earthquakes = PythonOperator(
   task_id="load_earthquakes",
   python_callable=load_earthquakes,
   dag=dag
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="datalake",
    sql='''
            CREATE TABLE IF NOT EXISTS quakes (
                id varchar(255), 
                magnitude real, 
                time bigint,
                type varchar(255),
                title varchar(255),
                coord_lat real,
                coord_long real,
                coord_height real
                );
        '''
)

save_earthquakes = PythonOperator(
    task_id="save_earthquakes",
    python_callable=save_earthquakes,
    dag=dag
)

earthquakes_validation = PostgresOperator(
    task_id="earthquakes_validation",
    dag=dag,
    postgres_conn_id='datalake',
    sql='''
		SELECT * FROM quakes ORDER BY TIME DESC LIMIT 10;
		SELECT COUNT(*) FROM quakes;
		'''
)


load_earthquakes >> create_table >> save_earthquakes >> earthquakes_validation
