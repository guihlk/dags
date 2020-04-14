from datetime import timedelta

import airflow

from airflow import DAG
from airflow.contrib.hooks.discord_webhook_hook import DiscordWebhookHook
from airflow.contrib.operators.discord_webhook_operator import DiscordWebhookOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

DISCORD_CONN_ID = 'discord-gui-hook'

def report_email(context, failure=False):
    subject = "Notification"
    if failure:
        subject = "{0} - Error".format(subject)
    send_email = EmailOperator(
        task_id='notification_email',
        to=Variable.get("notification_email"),
        subject=subject,
        html_content="Email notification, ok? testing"
    )
    send_email.execute(context)

def report_failure(context):
    report_email(context=context, failure=True)

def report_success(context):
    report_email(context=context, failure=False)
    

default_args = {
    'task_id': 'my_discord_dag',
    'owner': 'gui_airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': [Variable.get("notification_email")],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 4,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': report_failure,
    'on_success_callback': report_success

}

with DAG('My_DAG_WITH_DISCORD', default_args=default_args, description='My very second DAG is running right now', schedule_interval=timedelta(days=1)) as dag:
    discord_alert = DiscordWebhookOperator(
        http_conn_id=DISCORD_CONN_ID,
        message='Sample message - new bot',
        username='Gui',
        task_id='task_discord'
    )

    discord_alert
