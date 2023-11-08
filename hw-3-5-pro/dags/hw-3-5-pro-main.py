import airflow
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests as req
import psycopg2 as pg2
import datetime as dt
from datetime import datetime, timedelta

dag = DAG(
    "1t-hw-3-5-pro-main",
    default_args={
        "owner": "peter",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    description="1t-hw-3-5-pro-main",
    schedule=timedelta(minutes=5),
    start_date=datetime(2023, 10, 25),
    catchup=False,
    tags=["Data Engineer Course"],
)

def get_currencies_list():
    currencies = Variable.get("currencies", default_var="USD")
    return currencies.replace(' ','').split(',')

# Функция считает суммарное количество записей во всех таблицах 
# (вызывается дважды, до и после вызова API)

def count_db_records(ti):

    currencies_list = get_currencies_list()

    hook = PostgresHook(postgres_conn_id="user_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()
    db_record_counter = 0

    for cur in currencies_list:
        try:
            cursor.execute('SELECT COUNT(*) FROM {};'.format(cur))
            cfa = cursor.fetchall()[0][0]
            db_record_counter += cfa
        except:
            print('Error fetching data from table {}'.format(cur))
            return -1

    conn.commit()
    cursor.close()
    conn.close()

    return db_record_counter

# Посчитать и сохранить перед вызовом API

def count_db_records_before(ti):
    cnt = count_db_records(ti)
    ti.xcom_push("db_records_before", cnt)

# Вызов API

def get_a_quote(ti):

    # Исходные данные

    access_key = 'e5cf6dea5ff0c24020b6ff8be27f58f5'
    source = 'RUB'
    currencies = Variable.get("currencies", default_var="USD")
    currencies_list = get_currencies_list()

    # Запрос к Web API

    api_url = Variable.get("api_url") # "api.exchangerate.host"
    url = 'http://{}/live?access_key={}&source={}&currencies={}'.format(
        api_url, access_key, source, currencies)

    ti.xcom_push("data", "error")

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

    ti.xcom_push("data", data)
    ti.xcom_push("source", source)

# Обновление БД

def update_db(ti):

    data = ti.xcom_pull(task_ids="get_a_quote", key="data")
    if data == "error": return
    source = ti.xcom_pull(task_ids="get_a_quote", key="source")
    currencies_list = get_currencies_list()

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
               'rate float);'.format(cur)
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
            cur, dt.datetime.fromtimestamp(data['timestamp']), source, cur, rate)

        cursor.execute(insert_str)

    conn.commit()
    cursor.close()
    conn.close()

# Проверка обновления БД для сенсора

def is_db_updated(ti):
    db_records_now = count_db_records(ti)
    db_records_before = ti.xcom_pull(task_ids="count_db_records_before", key="db_records_before")
    return db_records_now != db_records_before

# Наполнение витрины

def data_mart(ti):

    print("in data mart!")

    source = ti.xcom_pull(task_ids="get_a_quote", key="source")
    currencies_list = get_currencies_list()

    # Создание витрины
    hook = PostgresHook(postgres_conn_id="user_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS data_mart('\
       'quote_timestamp timestamp,'\
       'source VARCHAR(4),'\
       'currency VARCHAR(4),'\
       'max_rate float,'\
       'min_rate float,'\
       'avg_rate float);'
    )

    # Очистка витрины
    cursor.execute('TRUNCATE TABLE data_mart;')

    # Наполнение витрины
    for cur in currencies_list:

        cursor.execute(
            'SELECT DISTINCT MAX(quote_timestamp) AS timestamp,'\
            ' MAX(rate) AS max_rate, MIN(rate) AS min_rate, AVG(rate) AS avg_rate '\
            'FROM {}'.format(cur))

        data = cursor.fetchall()[0]

        cursor.execute(
            "INSERT INTO data_mart VALUES('{}','{}','{}',{},{},{});".format(
                data[0], source, cur, data[1], data[2], data[3]))

        print(data)

    conn.commit()
    cursor.close()
    conn.close()


t0 = PythonOperator(
    task_id="count_db_records_before",
    python_callable=count_db_records_before,
    dag=dag,
)

t1 = PythonOperator(
    task_id="get_a_quote",
    python_callable=get_a_quote,
    dag=dag,
)

t2 = PythonOperator(
    task_id="update_db",
    python_callable=update_db,
    dag=dag,
)

t3 = PythonSensor(
    task_id="is_db_updated",
    poke_interval=30,
    timeout=70,
    soft_fail=True,
    mode="reschedule",
    python_callable=is_db_updated,
    dag=dag,
)

t4 = PythonOperator(
    task_id="data_mart",
    python_callable=data_mart,
    dag=dag,
)


t0 >> t1 >> t2 >> t3 >> t4