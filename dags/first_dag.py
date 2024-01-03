import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="first_dag",
    start_date=datetime.datetime(2023, 10, 13),
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo start!!",
    )

    task2 = BashOperator(
        task_id="task2",
        bash_command="echo finish!!",
    )

    task1 >> task2
