import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    "1t-hw-3-5-imports",
    default_args={
        "owner": "peter",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    description="import connections and variables",
    schedule="@once",
    start_date=datetime(2023, 10, 25),
    catchup=False,
    tags=["Data Engineer Course"],
)

t1 = BashOperator(
    task_id="import_connections_and_variables",
    bash_command="airflow connections import /opt/airflow/dags/connections.json && airflow variables import /opt/airflow/dags/variables.json",
    dag=dag,
)

t1