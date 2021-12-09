import datetime
import psycopg2
import psycopg2.extras
import requests
import time
import pandas as pd


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


def combine_tables_tweets_users():
    pg_hook, connection, cursor = conn_postgres()
    twitter = pd.read_sql("SELECT * FROM dl_twitter", connection)
    users = pd.read_sql("SELECT * from dl_twitterusers", connection)

    # remove duplicates from users
    users.drop_duplicates(subset=['id'], inplace=True)

    # merge tables
    df = pd.merge(twitter, users, how='left', left_on='author_id', right_on='id')
    df = df.rename(columns={"id_x": "tweet_id"}).drop(columns=['id_y', 'location'])

    # format date
    df['created_at'] = pd.to_datetime(df['created_at']).dt.tz_localize(None)
    # remove NaNs
    df = df[~df['username'].isna()]

    # write data to temp table for access
    df.to_sql('dw_temp', con=engine, if_exists='append', chunksize=1000, index=False)


def get_missing_data:
    pg_hook, connection, cursor = conn_postgres()
    BEARER_TOKEN = Variable.get("BEARER_TOKEN")
    # Setting filter to exclude certain usernames (mostly bots)
    FILTER_QUERY = Variable.get("FILTER_QUERY")

    start_time = '2017-07-01T00:00:00.000Z'
    end_time = "2017-12-31T23:59:59.000Z"
    query = "earthquake -minor, -is:reply -is:retweet {0}".format(FILTER_QUERY)
    max_results = "500"
    tweet_fields = "created_at,author_id"
    user_fields = 'username,location'
    expansions = 'author_id'
    counter = 0
    query_params = {'query': query, 'tweet.fields': tweet_fields, 'user.fields': user_fields, \
                    'start_time': start_time, 'end_time': end_time, 'max_results': max_results, \
                    'expansions': expansions}
    url = "https://api.twitter.com/2/tweets/search/all"
    headers = {"Authorization": "Bearer " + BEARER_TOKEN}

    lst_tweets = []
    lst_users = []
    iteration = 0
    while True:
        # get results according to url and query
        response = requests.request("GET", url, headers=headers, params=query_params)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        # combine data to one
        json_response = response.json()
        if 'data' in json_response:
            lst_tweets = lst_tweets + json_response['data']
            lst_users = lst_users + json_response['includes']['users']

        # check if more data available, if yes continue process
        if 'meta' in json_response:
            if 'next_token' in json_response['meta']:
                query_params['next_token'] = json_response['meta']['next_token']
                next_token = json_response['meta']['next_token']
                #  logging.info("Fetching next few tweets, next_token: ", query_params['next_token'])
                time.sleep(5)
            else:
                if iteration == 0:
                    query_params['start_time'] = '2018-07-01T00:00:00.000Z'
                    query_params['end_time'] = '2018-10-13T23:59:59.000Z'
                    iteration = 1
                    if 'next_token' in query_params:
                        del query_params['next_token']
                else:
                    if 'next_token' in query_params:
                        del query_params['next_token']
                    break
        else:
            if 'next_token' in query_params:
                del query_params['next_token']
                print('no meta in json_response')
            break

    # merge data
    twe = pd.DataFrame(lst_tweets)
    use = pd.DataFrame(lst_users)
    df = pd.merge(twe, use, how='left', left_on='author_id', right_on='id')
    df = df.rename(columns={"id_x": "tweet_id"}).drop(columns=['id_y'])
    df.drop(columns=['location'], inplace=True)
    df['created_at'] = pd.to_datetime(df['created_at']).dt.tz_localize(None)

    df.to_sql('dw_temp', con=engine, if_exists='append', chunksize=1000, index=False)

def remove_bots():
    pg_hook, connection, cursor = conn_postgres()
    df = pd.read_sql("SELECT * FROM dw_temp", connection)
    df.drop_duplicates(subset=['tweet_id'], inplace=True)

    # start with removing all users where username contains 'bot' or 'quake'
    remove = ['quake', 'bot']
    dw = df[~df.username.str.contains('|'.join(remove), case=False, na=False)]

    # remove users which posted more tweets than certain threshold
    post_counts = dw.username.value_counts()
    lst_user = post_counts.index.tolist()
    lst_count = post_counts.tolist()

    post_counts = pd.DataFrame(lst_user).rename(columns={0: 'username'})
    post_counts['value'] = lst_count

    bots = post_counts.loc[post_counts['value'] > 100].sort_values(by=['value'])
    bots = bots.username.tolist()

    dw = dw[~dw.username.str.contains('|'.join(bots), case=False, na=False)]
    dw.to_sql('dw_twitter', con=engine, if_exists='append', chunksize=1000, index=False)


dag = DAG(
    'clean_datalake
    start_date=datetime.datetime.now(),
    max_active_runs=1,
    schedule_interval=None
)

create_datawarehouse_table = PostgresOperator(
    task_id="create_datawarehouse_tables",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
        CREATE TABLE IF NOT EXISTS dw_twitter (
        text text, 
        author_id int,
        tweet_id int,
        created_at timestamp,
        username text,
        name text);
        
        CREATE TABLE IF NOT EXISTS dw_temp (
        text text, 
        author_id int,
        tweet_id int,
        created_at timestamp,
        username text,,
        name text);
        '''
)

combine_tables = PythonOperator(
    task_id='combine_tables_tweets_users',
    dag=dag,
    python_callable=combine_tables_tweets_users
)

get_twitter_data = PythonOperator(
    task_id='get_missing_data',
    dag=dag,
    python_callable=get_missing_data
)

clean_data = PythonOperator(
    task_id='remove_bots',
    dag=dag,
    python_callable=remove_bots
)


delete_table = PostgresOperator(
    task_id="tweets_validation",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
		DROP TABLE IF EXISTS dw_temp;
		'''
)



create_datawarehouse_table >> combine_tables >> clean_data >> delete_table
create_datawarehouse_table >> get_missing_data >> clean_data >> delete_table