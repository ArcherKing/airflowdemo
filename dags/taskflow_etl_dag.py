import json
from airflow.decorators import dag, task, task_group
from datetime import datetime


@dag(start_date=datetime(2023, 10, 24), schedule=None)
def taskflow_etl_dag():
    @task()
    def extract():
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
        return order_data

    @task_group()
    def transform(order_data):
        @task()
        def transform_sum(order_data_json):
            order_total = sum(
                [order_dict["order_price"] for order_dict in order_data_json]
            )
            return order_total

        @task
        def transform_count(order_data_json):
            order_count = len(order_data_json)
            return order_count

        @task
        def transform_averge(order_total, order_count):
            order_averge = order_total / order_count
            return order_averge

        order_averge_result = transform_averge(
            transform_sum(order_data),
            transform_count(order_data),
        )

        return order_averge_result

    @task
    def load(order_averge):
        print(f"Order Averge Price: {order_averge}")

    load(transform(extract()))


taskflow_etl_dag()
