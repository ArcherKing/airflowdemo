import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def extract(ti: PythonOperator):
    json_string = """
        [
            {
                "order_id": "1001",
                "order_item": "培根蛋餅",
                "order_price": 40
            },
            {
                "order_id": "1002",
                "oder_item": "大冰豆",
                "order_price": 20
            }
        ]
    """
    order_data = json.loads(json_string)
    ti.xcom_push(key="order_data", value=order_data)


def transform_sum(ti: PythonOperator):  # 計算總金額
    order_data_list = ti.xcom_pull(task_ids="extract", key="order_data")
    order_total = sum([order_dict["order_price"] for order_dict in order_data_list])
    ti.xcom_push(key="order_total", value=order_total)


def transform_count(ti: PythonOperator):  # 計算數量
    order_data_list = ti.xcom_pull(task_ids="extract", key="order_data")
    order_count = len(order_data_list)
    ti.xcom_push(key="order_count", value=order_count)


def transform_averge(ti: PythonOperator):  # 總金額 / 數量 = 平均金額
    order_total = ti.xcom_pull(task_ids="transform.transform_sum", key="order_total")
    order_count = ti.xcom_pull(task_ids="transform.transform_count", key="order_count")
    order_averge = order_total / order_count
    ti.xcom_push(key="order_averge", value=order_averge)


def load(ti: PythonOperator):
    order_averge = ti.xcom_pull(
        task_ids="transform.transform_averge", key="order_averge"
    )
    print(f"Averge Order Price: {order_averge}")


with DAG(
    dag_id="traditional_etl_dag",
    start_date=datetime(2023, 10, 24),
    schedule=None,
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )
    with TaskGroup(group_id="transform") as transform:
        transform_sum = PythonOperator(
            task_id="transform_sum",
            python_callable=transform_sum,
        )
        transform_count = PythonOperator(
            task_id="transform_count",
            python_callable=transform_count,
        )
        transform_averge = PythonOperator(
            task_id="transform_averge",
            python_callable=transform_averge,
        )
        [transform_sum, transform_count] >> transform_averge
    load = PythonOperator(
        task_id="load",
        python_callable=load,
    )
    extract >> transform >> load
