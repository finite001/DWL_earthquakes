import datetime
import psycopg2
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator




def conn_postgres():
    pg_hook = PostgresHook(postgres_conn_id="rds", schema="dbeq")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    return pg_hook, connection, cursor

def fill_timeframes():
    # connection to postgresql db
    pg_hook, connection, cursor = conn_postgres()

    data = [(1, '2019-10-01T00:00:00.000Z', '2019-12-31T23:59:59.000Z', False), \
            (2, '2019-07-01T00:00:00.000Z', '2019-09-30T23:59:59.000Z', False), \
            (3, '2019-04-01T00:00:00.000Z', '2019-06-30T23:59:59.000Z', False), \
            (4, '2019-01-01T00:00:00.000Z', '2019-03-31T23:59:59.000Z', False), \
            (5, '2018-10-01T00:00:00.000Z', '2018-12-31T23:59:59.000Z', False), \
            (6, '2018-07-01T00:00:00.000Z', '2018-09-30T23:59:59.000Z', False), \
            (7, '2018-04-01T00:00:00.000Z', '2018-06-30T23:59:59.000Z', False), \
            (8, '2018-01-01T00:00:00.000Z', '2018-03-31T23:59:59.000Z', False), \
            (9, '2017-10-01T00:00:00.000Z', '2017-12-31T23:59:59.000Z', False), \
            (10, '2017-07-01T00:00:00.000Z', '2017-09-30T23:59:59.000Z', False), \
            (11, '2017-04-01T00:00:00.000Z', '2017-06-30T23:59:59.000Z', False), \
            (12, '2017-01-01T00:00:00.000Z', '2017-03-31T23:59:59.000Z', False), \
            (13, '2016-10-01T00:00:00.000Z', '2016-12-31T23:59:59.000Z', False), \
            (14, '2016-07-01T00:00:00.000Z', '2016-09-30T23:59:59.000Z', False), \
            (15, '2016-04-01T00:00:00.000Z', '2016-06-30T23:59:59.000Z', False), \
            (16, '2016-01-01T00:00:00.000Z', '2016-03-31T23:59:59.000Z', False), \
            (17, '2015-10-01T00:00:00.000Z', '2015-12-31T23:59:59.000Z', False), \
            (18, '2015-07-01T00:00:00.000Z', '2015-09-30T23:59:59.000Z', False), \
            (19, '2015-04-01T00:00:00.000Z', '2015-06-30T23:59:59.000Z', False), \
            (20, '2015-01-01T00:00:00.000Z', '2015-03-31T23:59:59.000Z', False), \
            (21, '2014-10-01T00:00:00.000Z', '2014-12-31T23:59:59.000Z', False), \
            (22, '2014-07-01T00:00:00.000Z', '2014-09-30T23:59:59.000Z', False), \
            (23, '2014-04-01T00:00:00.000Z', '2014-06-30T23:59:59.000Z', False), \
            (24, '2014-01-01T00:00:00.000Z', '2014-03-31T23:59:59.000Z', False), \
            (25, '2013-10-01T00:00:00.000Z', '2013-12-31T23:59:59.000Z', False), \
            (26, '2013-07-01T00:00:00.000Z', '2013-09-30T23:59:59.000Z', False), \
            (27, '2013-04-01T00:00:00.000Z', '2013-06-30T23:59:59.000Z', False), \
            (28, '2013-01-01T00:00:00.000Z', '2013-03-31T23:59:59.000Z', False), \
            (29, '2012-10-01T00:00:00.000Z', '2012-12-31T23:59:59.000Z', False), \
            (30, '2012-07-01T00:00:00.000Z', '2012-09-30T23:59:59.000Z', False), \
            (31, '2012-04-01T00:00:00.000Z', '2012-06-30T23:59:59.000Z', False), \
            (32, '2012-01-01T00:00:00.000Z', '2012-03-31T23:59:59.000Z', False), \
            (33, '2011-10-01T00:00:00.000Z', '2011-12-31T23:59:59.000Z', False), \
            (34, '2011-07-01T00:00:00.000Z', '2011-09-30T23:59:59.000Z', False), \
            (35, '2011-04-01T00:00:00.000Z', '2011-06-30T23:59:59.000Z', False), \
            (36, '2011-01-01T00:00:00.000Z', '2011-03-31T23:59:59.000Z', False), \
            (37, '2010-10-01T00:00:00.000Z', '2010-12-31T23:59:59.000Z', False), \
            (38, '2010-07-01T00:00:00.000Z', '2010-09-30T23:59:59.000Z', False), \
            (39, '2010-04-01T00:00:00.000Z', '2010-06-30T23:59:59.000Z', False), \
            (40, '2010-01-01T00:00:00.000Z', '2010-03-31T23:59:59.000Z', False), \
            (41, '2020-10-01T00:00:00.000Z', '2010-12-31T23:59:59.000Z', False), \
            (42, '2020-07-01T00:00:00.000Z', '2010-09-30T23:59:59.000Z', False), \
            (43, '2020-04-01T00:00:00.000Z', '2010-06-30T23:59:59.000Z', False), \
            (44, '2020-01-01T00:00:00.000Z', '2010-03-31T23:59:59.000Z', False), \
            (45, '2021-10-01T00:00:00.000Z', '2010-10-30T23:59:59.000Z', False), \
            (46, '2021-07-01T00:00:00.000Z', '2010-09-30T23:59:59.000Z', False), \
            (47, '2021-04-01T00:00:00.000Z', '2010-06-30T23:59:59.000Z', False), \
            (48, '2021-01-01T00:00:00.000Z', '2010-03-31T23:59:59.000Z', False)]

    try:
        cur.executemany("""INSERT INTO timeframes (id, startdate, enddate, requested) 
                     VALUES (%s, %s, %s, %s)""", \
                        data)
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print(e)

    connection.commit()


dag = DAG(
    'create_timeframes',
    start_date=datetime.datetime.now(),
    on_success_callback=cleanup_xcom,
    schedule_interval=None
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id='rds',
    sql='''
        CREATE TABLE IF NOT EXISTS timeframes (
            id int,
            startdate text, 
            enddate text,
            requested boolean);
        '''
)

fill_table = PythonOperator(
    task_id='fill_timeframes',
    dag=dag,
    python_callable=fill_timeframes
)


create_table >> fill_table
