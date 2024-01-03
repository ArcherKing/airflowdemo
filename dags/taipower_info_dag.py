from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable
import json


taipower_info_col = [
    "update_time",
    "unit_type",
    "unit_name",
    "installed_capacity_mw",
    "net_generation_mw",
    "generation_to_capacity_ratio",
    "comments",
]


def process_taipower_info(**Kwargs):
    ti: PythonOperator = Kwargs["ti"]
    taipower_info = ti.xcom_pull(task_ids="task_get_taipower_info")
    taipower_info = json.loads(taipower_info)

    taipower_data = [
        (taipower_info[""], *unitinfo) for unitinfo in taipower_info["aaData"]
    ]
    taipower_sql_values = str(taipower_data)[1:-1]
    return taipower_sql_values


dag = DAG(
    dag_id="taipower_info_dag",
    start_date=datetime(2023, 10, 27),
    schedule="*/10 * * * *",
    catchup=False,
)

task_get_taipower_info = SimpleHttpOperator(
    task_id="task_get_taipower_info",
    method="GET",
    headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.69",
        "Content-Type": "application/json",
    },
    http_conn_id="taipower_info",
    extra_options={"verify": False},
    dag=dag,
)

task_process_taipower_info = PythonOperator(
    task_id="task_process_taipower_info",
    python_callable=process_taipower_info,
    dag=dag,
)

task_insert_taipower_data = SQLExecuteQueryOperator(
    task_id="task_insert_taipower_data",
    conn_id="taipower_postgres",
    # sql="sql/insert_taipower.sql",
    # params={"taipower_sql_values": task_process_taipower_info.output},
    sql=f"""
        INSERT INTO real_time_power_data (
        update_time,
        unit_type,
        unit_name,
        installed_capacity_mw,
        net_generation_mw,
        generation_to_capacity_ratio,
        comments
        ) VALUES {task_process_taipower_info.output};
    """,
    dag=dag,
)

# task_get_taipower_data = SQLExecuteQueryOperator(
#     task_id="task_get_taipower_data",
#     conn_id="taipower_postgres",
#     sql="SELECT * FROM real_time_power_data;",
#     dag=dag,
# )

(
    task_get_taipower_info
    >> task_process_taipower_info
    >> task_insert_taipower_data
    # >> task_get_taipower_data
)
