import datetime
import psycopg2
import psycopg2.extras
import requests
import time

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.db import provide_session
from airflow.models import XCom


def cleanup_xcom(context, session=None):
    dag_id = context["ti"]["dag_id"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


def conn_postgres():
    pg_hook = PostgresHook(postgres_conn_id="rds", schema="dbeq")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    return pg_hook, connection, cursor

def set_up_query(ti):
   # BEARER_TOKEN = Variable.get("BEARER_TOKEN")
    BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAJY%2BNgEAAAAA5SvyA6cq3jifLadZEWMEiYgUj%2FI%3Dci6sbF0x3Ya5EckuWJ6L7M4RJhptE16APFL9PMnhZPZXulQg0I"
    # Setting filter to exclude certain usernames (mostly bots)
    FILTER_QUERY = Variable.get("FILTER_QUERY")

    query = "earthquake -minor, -is:reply -is:retweet {0}".format(FILTER_QUERY)
    start_time = "2011-01-01T00:00:00.000Z"
    end_time = "2021-10-30T23:59:59.000Z"
    granularity = 'day'  # for daily counts
    query_params = {'query': query, 'start_time': start_time, 'end_time': end_time, \
                    'granularity': granularity}
    url = "https://api.twitter.com/2/tweets/counts/all"  # url provided by the API for the counts
    # define headers for authorization
    headers = {"Authorization": "Bearer " + BEARER_TOKEN}

    ti.xcom_push(key='query_params', value=query_params)
    ti.xcom_push(key='url', value=url)
    ti.xcom_push(key='headers', value=headers)


def get_twitter_data(ti):
    query_params = ti.xcom_pull(key='query_params', task_ids='set_up_query')
    url = ti.xcom_pull(key='url', task_ids='set_up_query')
    headers = ti.xcom_pull(key='headers', task_ids='set_up_query')
    tweets = []
    while True:
        # get results according to url and query
        response = requests.request("GET", url, headers=headers, params=query_params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        # combine data to one
        json_response = response.json()
        if 'data' in json_response:
            tweets = tweets + json_response['data']

        # check if more data available, if yes continue process
        if 'meta' in json_response:
            if 'next_token' in json_response['meta']:
                query_params['next_token'] = json_response['meta']['next_token']
                next_token = json_response['meta']['next_token']
               # logging.info("Fetching next few tweets, next_token: ", query_params['next_token'])
                time.sleep(4)
            else:
                if 'next_token' in query_params:
                    del query_params['next_token']
                break
        else:
            if 'next_token' in query_params:
                del query_params['next_token']
            break
    ti.xcom_push(key='tweets', value=tweets)



def load_tweetcount_to_rds(ti):
    tweets = ti.xcom_pull(key='tweets', task_ids='get_twitter_data')
    iter_tweets = iter(tweets)

    # connection to postgresql db
    pg_hook, connection, cursor = conn_postgres()

    # insert tweets
    psycopg2.extras.execute_batch(cursor, """INSERT INTO tweetcount VALUES(
    %(start)s,
    %(end)s,
    %(tweet_count)s
    );""", iter_tweets)

    connection.commit()


dag = DAG(
    'get_twitter_count',
    start_date=datetime.datetime.now(),
    on_success_callback=cleanup_xcom,
    max_active_runs=1,
    schedule_interval=None
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
        CREATE TABLE IF NOT EXISTS tweetcount (
        starttime text, 
        endtime text,
        count int);
        '''
)

create_query = PythonOperator(
    task_id='set_up_query',
    dag=dag,
    python_callable=set_up_query
)

access_twitterAPI = PythonOperator(
    task_id='get_twitter_data',
    dag=dag,
    python_callable=get_twitter_data
)

load_tweetcount = PythonOperator(
    task_id='load_tweetcount_to_rds',
    dag=dag,
    python_callable=load_tweetcount_to_rds
)


tweet_validation = PostgresOperator(
    task_id="tweets_validation",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
		SELECT * FROM tweetcount limit 10;
		'''
)



create_table >> create_query >> access_twitterAPI >> load_tweetcount >> tweet_validation
