from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.taskinstance import TaskInstance
from datetime import datetime
import json
import os
import re
from typing import Dict
import requests


def process_read_history(mode, ti: TaskInstance):
    """讀取或更新閱讀紀錄"""

    read_history_path = os.path.join("airflow/data/read_history.json")
    if mode == "read":
        with open(read_history_path, "r", encoding="utf-8") as fp:
            read_history = json.load(fp)
        return dict(read_history)
    elif mode == "update":
        all_read_info: Dict = ti.xcom_pull(
            task_ids="check_roman_info_task", key="all_read_info"
        )

        with open(read_history_path, "w", encoding="utf-8") as fp:
            all_read_history = {
                key: {
                    "name": value["name"],
                    "previous_chapter_num": value["previous_chapter_num"],
                }
                for key, value in all_read_info.items()
            }
            fp.write(json.dumps(all_read_history, ensure_ascii=False, indent=4))


def get_read_bookids(ti: TaskInstance):
    read_history: Dict = ti.xcom_pull("get_read_history_task")
    bookids_str = ",".join(read_history.keys())
    print("bookids_str:", bookids_str)
    return bookids_str


def check_roman_info(ti: TaskInstance):
    read_history: Dict = ti.xcom_pull("get_read_history_task")

    with open("airflow/data/crawl_latest.jsonl", "r", encoding="utf-8") as fp:
        all_latest_info = {
            crawl_latest["bookid"]: crawl_latest["latest_info"]
            for crawl_latest_json in fp.readlines()
            if (crawl_latest := json.loads(crawl_latest_json)).get("bookid")
        }

    all_read_info = read_history
    for bookid, roman_info in all_read_info.items():
        latest_chapter_num = all_latest_info[bookid]["text"].split(" ")[0]
        previous_chapter_num = roman_info["previous_chapter_num"]
        roman_info = all_latest_info[bookid]
        roman_info["updated"] = previous_chapter_num != latest_chapter_num
        if roman_info["updated"]:
            roman_info["previous_chapter_num"] = latest_chapter_num
        all_read_info[bookid].update(roman_info)

    ti.xcom_push(key="all_read_info", value=all_read_info)
    is_updated = any([roman_info["updated"] for roman_info in all_read_info.values()])
    return "generate_message_task" if is_updated else "no_do_nothing_task"


def generate_message(ti: TaskInstance):
    all_read_info: Dict = ti.xcom_pull(
        task_ids="check_roman_info_task", key="all_read_info"
    )
    messages = []
    for bookid, roman_info in all_read_info.items():
        if not roman_info["updated"]:
            continue

        title = roman_info["title"]
        href = f'https://tw.mingzw.net{roman_info["href"]}'
        try:
            pattern = re.compile(
                r"小說名:(?P<bookname>.*?) 作者:(?P<author>.*?) 章節名:(?P<episode_name>.*?) 更新時間:(?P<update_date>.*?)$"
            )
            match = pattern.search(title)
            data = match.groupdict()
            data = {key: value.strip() for key, value in data.items()}
            message = f"😏{data['update_date']} {data['bookname']} {data['episode_name']} {data['author']} {href}\n"
        except Exception as e:
            print(e)
            message = f"🙄{title} {href}"

        messages.append(message)

    with open("airflow/data/message.txt", "w", encoding="utf-8") as fp:
        fp.writelines(messages)


def send_message():
    line_notify_url = "https://notify-api.line.me/api/notify"
    token = "yourtoken"
    with open("airflow/data/message.txt", "r", encoding="utf-8") as fp:
        for line in fp:
            headers = {"Authorization": f"Bearer {token}"}
            data = {"message": line}
            r = requests.post(
                line_notify_url,
                data=data,
                headers=headers,
            )
            print(r.json())


dag = DAG(
    dag_id="crawl_roman_dag",
    start_date=datetime(2023, 12, 1),
    schedule="*/5 * * * *",
    catchup=False,
)

get_read_history_task = PythonOperator(
    task_id="get_read_history_task",
    python_callable=process_read_history,
    op_args=["read"],
    dag=dag,
)

get_read_bookids_task = PythonOperator(
    task_id="get_read_bookids_task",
    python_callable=get_read_bookids,
    dag=dag,
)

# cd ~/airflow/crawl_roman;scrapy runspider crawl_roman/spiders/mingzw.py -a bookids=12345,77777
get_roman_info_task = BashOperator(
    task_id="get_roman_info_task",
    bash_command=(
        "cd ~/airflow/crawl_roman;scrapy crawl mingzw"
        " -a bookids=\"{{ti.xcom_pull('get_read_bookids_task')}}\""
    ),
    dag=dag,
)

#
check_roman_info_task = BranchPythonOperator(
    task_id="check_roman_info_task",
    python_callable=check_roman_info,
    dag=dag,
)

generate_message_task = PythonOperator(
    task_id="generate_message_task",
    python_callable=generate_message,
    dag=dag,
)

send_message_task = PythonOperator(
    task_id="send_message_task",
    python_callable=send_message,
    dag=dag,
)

update_read_history_task = PythonOperator(
    task_id="update_read_history_task",
    python_callable=process_read_history,
    op_args=["update"],
    dag=dag,
)

no_do_nothing_task = EmptyOperator(
    task_id="no_do_nothing_task",
)

(
    get_read_history_task
    >> get_read_bookids_task
    >> get_roman_info_task
    >> check_roman_info_task
)
(
    check_roman_info_task
    >> generate_message_task
    >> send_message_task
    >> update_read_history_task
)
check_roman_info_task >> no_do_nothing_task
