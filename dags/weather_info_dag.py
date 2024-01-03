from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
import requests

from typing import Dict, List


@dag(
    start_date=datetime(2100, 1, 1),
    schedule=None,
    catchup=False,
)
def weather_info_dag():
    @task
    def get_data():
        """
        取得氣象資料
        """
        weather_info = Variable.get("weather_info", deserialize_json=True)
        url = weather_info["url"]
        headers = {
            "authorization": weather_info["authorization"],
        }

        response = requests.get(url=url, headers=headers)
        print(response.text)

    @task
    def process_data():
        """
        清洗氣象資料
        """

    @task
    def insert_postgres():
        """
        新增至資料庫
        """

    @task
    def generate_message():
        """
        生成通知訊息
        """

    @task
    def send_notification():
        """
        使用line通知
        """

    get_data()
    process_data()
    insert_postgres()
    generate_message()
    send_notification()


weather_info_dag()
