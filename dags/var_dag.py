from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def set_var():
    Variable.set("my_key1", "my_value")
    Variable.set("my_key2", {"key_in_json": "value_in_json"}, serialize_json=True)


def get_var():
    my_var = Variable.get("my_key1")
    my_json_var = Variable.get("my_key2", deserialize_json=True)["key_in_json"]
    print(my_var)
    print(my_json_var)


def get_var_by_context(**context):
    my_var = context["var"]["value"].get("my_key1")
    my_json_var = context["var"]["json"].get("my_key2")["key_in_json"]
    print(f"context my_var: {my_var}")
    print(f"context my_json: {my_json_var}")


def print_context_func(**context):
    print(context)


with DAG(
    dag_id="var_dag",
    start_date=datetime(2023, 10, 17),
    schedule_interval=None,
) as dag:
    task1 = PythonOperator(
        task_id="set_var",
        python_callable=set_var,
    )
    task2 = PythonOperator(
        task_id="get_var",
        python_callable=get_var,
    )
    task3 = PythonOperator(
        task_id="get_var_by_context",
        python_callable=get_var_by_context,
    )
    task4 = PythonOperator(
        task_id="print_context_func",
        python_callable=print_context_func,
    )

    task1 >> [task2, task3] >> task4
