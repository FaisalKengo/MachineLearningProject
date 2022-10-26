import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd

def csvToJson():
    df = pd.read_csv('/mnt/d/DokumenFaisal/PersonalCodeProject/MachineLearningProject/Dataset/csgo_games.csv')
    for i,r in df.iterrows():
        print(r['match_date'])
    df.to_json('fromAirflow.json', orient = 'records')

default_args = {
    'owner': 'FaisalKengo',
    'start_date': dt.datetime(2022, 10, 25),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with DAG('CSGO_MatchTracker_DAG',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),      # '0 * * * *',
         ) as dag:

    print_starting = BashOperator(task_id='starting',
                               bash_command='echo "Reading CSV file..."')
    
    csvJson = PythonOperator(task_id='convertCSVtoJson',
                                 python_callable=csvToJson)


print_starting >> csvJson
