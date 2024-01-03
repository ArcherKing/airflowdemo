from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="task_group_dag",
    start_date=datetime(2023, 10, 18),
    schedule=None,
) as dag:
    with TaskGroup(group_id="my_task_group") as my_task_group:
        task1 = EmptyOperator(task_id="task1")
        task2 = EmptyOperator(task_id="task2")
        task3 = EmptyOperator(task_id="task3")

        task1 >> task2 >> task3

    start_task = EmptyOperator(task_id="start_task", dag=dag)
    end_task = EmptyOperator(task_id="end_task", dag=dag)

    start_task >> my_task_group >> end_task
