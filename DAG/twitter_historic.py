import datetime
import psycopg2
import psycopg2.extras
import requests

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def conn_postgres():
    pg_hook = PostgresHook(postgres_conn_id="rds", schema="dbeq")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    connection.set_session(autocommit=True)

    return pg_hook, connection, cursor

def set_up_query(ti):
    BEARER_TOKEN = Variable.get("BEARER_TOKEN")
    # Setting filter to exclude certain usernames (mostly bots)
    FILTER_QUERY = Variable.get("FILTER_QUERY")
    # connection to postgresql db
    pg_hook, connection, cursor = conn_postgres()

    # define query params
    cursor.execute("SELECT startdate FROM timeframes ORDER BY id asc LIMIT 1;")
    start_time = cursor.fetchone()
    start_time = start_time[0]
    cursor.execute("SELECT enddate FROM timeframes ORDER BY id asc LIMIT 1;")
    end_time = cursor.fetchone()
    end_time = end_time[0]
    cursor.execute("""
                    DELETE FROM timeframes 
                    WHERE ctid IN (SELECT ctid 
                                FROM timeframes
                                ORDER BY ID asc
                                LIMIT 1;
                """)
    query = "earthquake -minor, -is:reply -is:retweet {0}".format(FILTER_QUERY)
    max_results = "500"
    tweet_fields = "created_at,author_id"
    user_fields = 'username,location'
    expansions = 'author_id'
    query_params = {'query': query, 'tweet.fields': tweet_fields, 'user.fields': user_fields, \
                    'start_time': start_time, 'end_time': end_time, 'max_results': max_results, \
                    'expansions': expansions}
    url = "https://api.twitter.com/2/tweets/search/all"
    headers = {"Authorization": "Bearer " + BEARER_TOKEN}

    ti.xcom_push(key='query_params', value=query_params)
    ti.xcom_push(key='url', value=url)
    ti.xcom_push(key='headers', value=headers)


def get_twitter_data(ti):
    query_params = ti.xcom_pull(key='query_params', task_ids='set_up_query')
    url = ti.xcom_pull(key='url', task_ids='set_up_query')
    headers = ti.xcom_pull(key='headers', task_ids='set_up_query')
    tweets = []
    users = []
    while True:
        # get results according to url and query
        response = requests.request("GET", url, headers=headers, params=query_params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        # combine data to one
        json_response = response.json()
        if 'data' in json_response:
            tweets = tweets + json_response['data']
            users = users + json_response['includes']['users']

        # check if more data available, if yes continue process
        if 'meta' in json_response:
            if 'next_token' in json_response['meta']:
                query_params['next_token'] = json_response['meta']['next_token']
                next_token = json_response['meta']['next_token']
                logging.info("Fetching next few tweets, next_token: ", query_params['next_token'])
                time.sleep(3)
            else:
                if 'next_token' in query_params:
                    del query_params['next_token']
                break
        else:
            if 'next_token' in query_params:
                del query_params['next_token']
            break
    ti.xcom_push(key='tweets', value=tweets)
    ti.xcom_push(key='users', value=users)


def load_tweets_to_rds(ti):
    tweets = ti.xcom_pull(key='tweets', task_ids='get_twitter_data')
    iter_tweets = iter(tweets)

    # connection to postgresql db
    pg_hook, connection, cursor = conn_postgres()

    # insert tweets
    psycopg2.extras.execute_batch(cursor, """INSERT INTO tweets VALUES(
    %(text)s,
    %(author_id)s,
    %(id)s,
    %(created_at)s
    );""", iter_tweets)



def load_users_to_rds(ti):
    users = ti.xcom_pull(key='users', task_ids='get_twitter_data')
    # add location to all users, empty string if element does not exist (to insert data into table)
    for item in users:
        if 'location' not in item:
            item['location'] = ""
    iter_users = iter(users)

    # connection to postgresql db
    pg_hook, connection, cursor = conn_postgres()

    # insert users
    psycopg2.extras.execute_batch(cursor, """INSERT INTO tweets_user VALUES(
    %(id)s,
    %(username)s,
    %(name)s,
    %(location)s
    );""", iter_users)

dag = DAG(
    'twitter_historic',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    max_active_runs=1,
    schedule_interval='0 */5 * * *'  # every 5 hours
)

create_table = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
        CREATE TABLE IF NOT EXISTS tweets (
        text text, 
        author_id bigint,
        id bigint,
        created_at text);
        CREATE TABLE IF NOT EXISTS tweets_user (
        id bigint, 
        username text, 
        name text, 
        location text);
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

load_tweets = PythonOperator(
    task_id='load_tweets_to_rds',
    dag=dag,
    python_callable=load_tweets_to_rds
)

load_users = PythonOperator(
    task_id='load_users_to_rds',
    dag=dag,
    python_callable=load_users_to_rds
)

tweet_validation = PostgresOperator(
    task_id="tweets_validation",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
		SELECT * FROM tweets limit 10;
		SELECT COUNT(*) FROM tweets;
		'''
)

user_validation = PostgresOperator(
    task_id="users_validation",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
        SELECT * FROM tweets_user limit 10;
        '''
)

create_table >> create_query >> access_twitterAPI >> load_tweets >> tweet_validation
create_table >> create_query >> access_twitterAPI >> load_users >> user_validation
