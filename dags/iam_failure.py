from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date = datetime(2024,3,1),
    schedule=None,
    catchup=False,
)
def iam_failure():

    @task()
    def failure_task():
        raise Exception("I AM FAILURE !!!")

    failure_task()
iam_failure()