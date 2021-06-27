from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from extract_transform_emails import *
from email_assistant import *
import airflow


# define the arguments for the DAG
default_args = {
    'owner': 'Sophie',
    'depends_on_past': True,
    'start_date':airflow.utils.dates.days_ago(1),
    'email': ['sophanna.ek@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30)
}

## define the etl functions 
def etl(): 
    # extract, transform, load
    mails_df = extract_transform_emails()
    load(mails_df)


def generate_report(): 
    generate_excel()



with DAG('email_asistant', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    etl = PythonOperator(task_id='etl',python_callable=etl)
    generate_report = PythonOperator(task_id='generate_report',python_callable=generate_report)

    etl >> generate_report
    
