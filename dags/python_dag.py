from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello(ti: PythonOperator):
    name = ti.xcom_pull(task_ids="send_name", key="name")
    print(f"Hallo {name}, guten Tag.")


def send_name(ti: PythonOperator):
    ti = ti.xcom_push(key="name", value="Archer")


with DAG(
    dag_id="python_dag",
    start_date=datetime(2023, 10, 13),
    schedule_interval=None,
) as dag:
    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )
    task2 = PythonOperator(
        task_id="send_name",
        python_callable=send_name,
    )

    task2 >> task1
