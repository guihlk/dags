from datetime import timedelta

import airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'task_id': 'my_dag_with_email',
    'owner' : 'gui_airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'email' : ['guilherme@holdack.eu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 4,
    'retry_delay': timedelta(seconds=10)
    }

dag = DAG(
    'My_dag_with_email',
    default_args=default_args,
    description='My very first DAG is running right now',
    schedule_interval=timedelta(minutes=2),
)

email_notification = EmailOperator(
    task_id='email_op',
    to='guilherme@holdack.com.br',
    subject='Example of notification - Airflow',
    html_content='Content of the email, no HTML formatting whatsoever',
    dag=dag

)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)
t1 >> email_notification