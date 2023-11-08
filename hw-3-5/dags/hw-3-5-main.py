import airflow
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests as req
import psycopg2 as pg2
import datetime as dt
from datetime import datetime, timedelta

dag = DAG(
    "1t-hw-3-5-main",
    default_args={
        "owner": "peter",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    description="1t-hw-3-5",
    schedule=timedelta(minutes=10),
    start_date=datetime(2023, 10, 25),
    catchup=False,
    tags=["Data Engineer Course"],
)

def get_quotes():

    # Исходные данные

    access_key = 'e5cf6dea5ff0c24020b6ff8be27f58f5'
    source = 'RUB'
    currencies = Variable.get("currencies")
    currencies_list = currencies.replace(' ','').split(',')

    # Запрос к Web API

    api_url = Variable.get("api_url") # "api.exchangerate.host"
    url = 'http://{}/live?access_key={}&source={}&currencies={}'.format(
        api_url, access_key, source, currencies)

    try:
        response = req.get(url)
    except:
        print('API error - no data fetched and recorded')
        return

    data = response.json()

    if data['success'] != True or response.status_code != 200:
        print('API error - no data fetched and recorded')
        return
    else: print('API OK')

    # Создание таблиц

    hook = PostgresHook(postgres_conn_id="user_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for cur in currencies_list:
        cursor.execute('CREATE TABLE IF NOT EXISTS {}('\
               'id SERIAL PRIMARY KEY,'\
               'quote_timestamp timestamp,'\
               'source VARCHAR(4),'\
               'currency VARCHAR(4),'\
               'rate float);'.format(source + cur)
              )

    conn.commit()
    cursor.close()
    conn.close()

    # Наполнение таблиц

    hook = PostgresHook(postgres_conn_id="user_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for cur in currencies_list:

        cur_pair = source + cur
        rate = data['quotes'][cur_pair]
        if rate > 0: rate = 1.0/rate
        else: rate = 'NULL'
    
        insert_str = "INSERT INTO {} (quote_timestamp,source,currency,rate) VALUES ('{}','{}','{}',{});".format(
            cur_pair, dt.datetime.fromtimestamp(data['timestamp']), source, cur, rate)

        cursor.execute(insert_str)

    conn.commit()
    cursor.close()
    conn.close()

t2 = PythonOperator(
    task_id="get_a_quote",
    provide_context=True,
    python_callable=get_quotes,
    dag=dag,
)

t2