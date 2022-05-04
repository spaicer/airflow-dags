'''DAG for SPAICER demo realizing a data pipeline'''


import json
from datetime import datetime
import requests
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG


# endpoints

URL_FETCH = 'https://philipp.app.dezem.io/spaicer-data'
URL_FORWARD = 'https://philipp.app.dezem.io/spaicer-results'
URL_MODULE = 'https://philipp.app.dezem.io/spaicer-module'


# function definitions

def _fetch_data():
    '''fetch data from external service'''

    try:
        url = URL_FETCH

        payload = {}
        headers = {}

        response = requests.request('GET', url, headers=headers, data=payload)

        values = json.loads(response.text)['values']
    except:
        return False

    # emulate error while fetching
    if datetime.now().second >= 30:
        return values
    return False


def _fetch_success(ti):
    '''verify success of data fetching'''
    fetched_data = ti.xcom_pull(task_ids=['fetch_data'])
    print('fetched_data', fetched_data)
    if fetched_data[0]:
        return 'process_data_with_ai_module'
    return 'alert'


def _alert():
    '''dummy function for alerting'''
    print('Send alert e-mail now!')


def _process_data_with_ai_module(ti):
    '''push data to AI module'''
    data_array = ti.xcom_pull(task_ids=['fetch_data'])

    url = URL_MODULE

    payload = json.dumps({'values': data_array})
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request('POST', url, headers=headers, data=payload)

    return json.loads(response.text)['result']


def _forward_results(ti):
    '''forward results'''
    results = ti.xcom_pull(task_ids='process_data_with_ai_module')

    url = URL_FORWARD

    payload = json.dumps({'result': results})
    headers = {
        'Content-Type': 'application/json'
    }

    requests.request('POST', url, headers=headers, data=payload)


# DAG definition

with DAG(
    'spaicer_demo_dag',
    start_date=datetime(2022, 1, 3),
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=_fetch_data
    )

    alert = PythonOperator(
        task_id='alert',
        python_callable=_alert
    )

    process_data_with_ai_module = PythonOperator(
        task_id='process_data_with_ai_module',
        python_callable=_process_data_with_ai_module
    )

    forward_results = PythonOperator(
        task_id='forward_results',
        python_callable=_forward_results
    )

    fetch_success = BranchPythonOperator(
        task_id='fetch_success',
        python_callable=_fetch_success
    )

# flow definition
    fetch_data >> fetch_success >> [
        forward_results << process_data_with_ai_module,
        alert
    ]

