import datetime
import logging
import requests
import json
import boto3
import sys
import os
import csv
import io

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


#initialising the dag
def start():
    logging.info('Starting the DAG_')


#Access the api and pushing the return into the xcom storage
def get_data_from_api(ti):
    starttime = '2010-01-01'
    minimum_magnitude = '6'
    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime='+starttime+'&minmagnitude='+minimum_magnitude
    r = requests.get(url)
    r_json = json.loads(r.text)["features"]
    ti.xcom_push(key="major_historical_earthquakes", value = r_json)
    logging.info(starttime)
    logging.info(len(r_json))


#Accessing the S3 buckets using boto3 client and safe json file
def S3_credentials(ti):
    s3_client = boto3.client('s3')
    s3_bucket_name = 'earthquakebucket'
    s3 = boto3.resource('s3',
                        aws_access_key_id='<acces-key>',
                        aws_secret_access_key='<secret-access-key>',
                        aws_session_token='<session-token>')
    earthquakes1 = ti.xcom_pull(key="major_historical_earthquakes")
    logging.info(earthquakes1)

    #inserting json file in s3 bucket
    s3object = s3.Object("earthquakebucket", "earthquake.json")
    s3object.put(
        Body=(bytes(json.dumps(earthquakes1).encode('UTF-8')))
    )

#Creating the dag and defining daily schedule
dag = DAG(
        "S3-dag",
        schedule_interval="@daily",
        start_date=datetime.datetime.now()- datetime.timedelta(days=1))


startup_dag = PythonOperator(
   task_id="startup_dag",
   python_callable=start,
   dag=dag
)


access_api = PythonOperator(
   task_id="access_api",
   python_callable=get_data_from_api,
   dag=dag
)


access_s3 = PythonOperator(
   task_id="access_s3",
   python_callable=S3_credentials,
   dag=dag
)


startup_dag >> access_api >> access_s3