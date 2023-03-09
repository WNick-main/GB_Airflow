import os
import datetime as dt
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task

default_args = {
    'owner': 'wn',
    'start_date': dt.datetime(2022, 5, 14),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

@dag(
    dag_id='titanic_taskflow',
    schedule_interval=None,
    default_args=default_args,
)
def titanic_etl():

    @task
    def download_titanic_dataset():
        url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
        df = pd.read_csv(url)
        path = get_path('titanic.csv')
        df.to_csv(path, encoding='utf-8')
        return path

    @task
    def pivot_dataset(path):
        titanic_df = pd.read_csv(path)
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Fare',
                                    aggfunc='mean').reset_index()

        df.to_csv(get_path('titanic_pivot2.csv'))

    @task
    def mean_fare_per_class(path):
        titanic_df = pd.read_csv(path)
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Fare',
                                    aggfunc='mean').reset_index()

        df.to_csv(get_path('titanic_mean_fares.csv'))

    last_task = BashOperator(
        task_id='last_step',
        bash_command='echo "Pipeline finished! Execution date is {{ execution_date }}"'
    )

    path = download_titanic_dataset()
    pivot_dataset(path) >> last_task
    mean_fare_per_class(path) >> last_task

titanic_taskflow_dag = titanic_etl()
