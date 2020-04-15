import datetime
from datetime import timedelta
import traceback

import airflow
from airflow import DAG
from airflow.contrib.hooks.discord_webhook_hook import DiscordWebhookHook
from airflow.contrib.operators.discord_webhook_operator import DiscordWebhookOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.utils.email import send_email

DISCORD_CONN_ID = 'discord-hook'
NOTIFICATIONS_CONFIG = Variable.get("notifications_config", deserialize_json=True)
DISCORD_CONFIG = Variable.get("discord_config", deserialize_json=True)

def report_callback(context, subject, body):
    send_email = EmailOperator(
        task_id='notification_email',
        to=NOTIFICATIONS_CONFIG["email"],
        subject=subject,
        html_content=body
    )
    send_email.execute(context)


def report_failure(context):
    now = context["execution_date"]
    subject = NOTIFICATIONS_CONFIG["error_vars"]["subject"]
    exception = context['exception']
    formatted_exception = ''.join(
        traceback.format_exception(etype=type(exception),
                                value=exception, tb=exception.__traceback__
                                )
    ).strip()
    content = NOTIFICATIONS_CONFIG["error_vars"]["content"].format(
        context['task_instance'].dag_id,
        context['task_instance'].task_id,
        now.strftime("%d.%m.%Y - %H:%M:%S"),
        formatted_exception)
    report_callback(context=context, subject=subject, body=content)


def report_success(context):
    now = context["execution_date"]
    subject = NOTIFICATIONS_CONFIG["success_vars"]["subject"]
    content = NOTIFICATIONS_CONFIG["success_vars"]["content"].format(
        context['task_instance'].dag_id,
        context['task_instance'].task_id,
        now.strftime("%d.%m.%Y - %H:%M:%S"))
    report_callback(context=context, subject=subject, body=content)


def report_retry(context):
    now = context["execution_date"]
    subject = NOTIFICATIONS_CONFIG["retry_vars"]["subject"]
    content = NOTIFICATIONS_CONFIG["retry_vars"]["content"].format(
        context['task_instance'].dag_id,
        context['task_instance'].task_id,
        now.strftime("%d.%m.%Y - %H:%M:%S"))
    report_callback(context=context, subject=subject, body=content)
    

default_args = {
    'task_id': 'my_discord_dag',
    'owner': 'gui_airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': [NOTIFICATIONS_CONFIG["email"]],
    'email_on_failure': False,
    'email_on_success': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(seconds=10),

}

with DAG('DISCORD_WEBHOOK_INTEGRATION', default_args=default_args, description='A DAG integrated with Discord by webhooks. Custom callbacks to show integration working', schedule_interval=timedelta(minutes=1)) as dag:

    discord_alert = DiscordWebhookOperator(
        http_conn_id=DISCORD_CONN_ID,
        message='Sample message - webhook triggered - {}'.format(
            datetime.datetime.now().strftime("%d.%m.%Y - %H:%M:%S")),
        username=DISCORD_CONFIG["username"],
        task_id='task_discord',
        on_failure_callback=report_failure,
        on_success_callback=report_success,
        on_retry_callback=report_retry
    )

    discord_alert
