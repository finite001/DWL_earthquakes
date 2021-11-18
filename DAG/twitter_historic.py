import datetime
import psycopg2
import psycopg2.extras
import requests

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def set_up_query(ti):
    BEARER_TOKEN = Variable.get("BEARER_TOKEN")
    # Setting filter to exclude certain usernames (hardcoded here, but extracted with
    # code from test query)
    filter_names_query = "-from:quakeupdates -from:jojo2727 -from:MonitorSismico -from:MyComicalLife -from:news_sokuho_bot -from:DiariosRobot -from:EN_NERV -from:GDACS -from:earthquake_jp -from:EQAlerts -from:j1_quake -from:iSachinSrivstva -from:VolcanoEWS -from:ChileAlertaApp -from:earthb0t -from:sexy_vegetables -from:zishin3255 -from:everyEarthquake -from:MapQuake -from:swap_bot_bash -from:eq_map -from:eq_map_es -from:eq_map_ww -from:SEISMOinfo -from:VegaBajaWx -from:WatchOurCity -from:Keith_Event -from:SismoDetector -from:cvb_223 -from:ExBulletinUk -from:EMSC -from:StoixeioJewelry -from:megamodo -from:earthquakevt -from:QuakeBotter -from:twtaka_jp -from:EarthquakeTw -from:ENSO1998 -from:eq_map_ww2 -from:eq_map_es2"

    # start_time = ["2021-07-01T00:00:00.000Z", "2021-01-01T00:00:00.000Z", "2020-07-01T00:00:00.000Z", \
    #               "2020-01-01T00:00:00.000Z", "2019-07-01T00:00:00.000Z", "2019-01-01T00:00:00.000Z", \
    #               "2018-07-01T00:00:00.000Z", "2018-01-01T00:00:00.000Z", "2017-07-01T00:00:00.000Z", \
    #               "2017-01-01T00:00:00.000Z", "2016-07-01T00:00:00.000Z", "2016-01-01T00:00:00.000Z",]
    # end_time = ["2021-10-31T23:59:59.000Z", "2021-06-30T23:59:59.000Z", "2020-12-31T23:59:59.000Z", \
    #               "2020-06-30T23:59:59.000Z", "2019-12-31T23:59:59.000Z", "2019-06-30T23:59:59.000Z", \
    #               "2018-12-31T23:59:59.000Z", "2018-06-30T23:59:59.000Z", "2017-12-31T23:59:59.000Z", \
    #               "2017-06-30T23:59:59.000Z", "2016-12-31T23:59:59.000Z", "2016-06-30T23:59:59.000Z",]

    # define query params
    query = "earthquake -minor, -is:reply -is:retweet {0}".format(filter_names_query)
    start_time = "2021-11-15T12:00:00.000Z"
    end_time = "2021-11-15T12:49:59.000Z"
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
                print("Fetching next few tweets, next_token: ", query_params['next_token'])
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


def load_data_to_rds(ti):
    tweets = ti.xcom_pull(key='tweets', task_ids='get_twitter_data')
    users = ti.xcom_pull(key='users', task_ids='get_twitter_data')
    # add location to all users, empty string if element does not exist (to insert data into table)
    for item in users:
        if 'location' in item:
            pass
        else:
            item['location'] = ""

    # create iterators
    iter_tweets = iter(tweets)
    iter_users = iter(users)

    # connection to postgresql db
    pg_hook = PostgresHook(postgres_conn_id="rds", schema="dbeq")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    connection.set_session(autocommit=True)
    # insert tweets
    psycopg2.extras.execute_batch(cursor, """INSERT INTO tweets VALUES(
    %(text)s,
    %(author_id)s,
    %(id)s,
    %(created_at)s
    );""", iter_tweets)

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

load_data = PythonOperator(
    task_id='load_data_to_rds',
    dag=dag,
    python_callable=load_data_to_rds
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

create_table >> create_query >> access_twitterAPI >> load_data >> tweet_validation >> user_validation
