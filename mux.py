import os,sys
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
from datetime import timedelta
from textwrap import dedent
import json
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from libs.mux import feedProviders,feedItems,prepareMux,doMux

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
TAGS = ['vod','import']
SCHEDULE_INTERVAL = None
START_DATE = datetime.now() - timedelta(days=1)
NO_OF_PARALLEL_CMDS = int(Variable.get('mux_no_of_parallel_cmds'))

default_args = {
    'owner': 'smartivus',
    'email': ['support@smartivus.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'tags': TAGS,
    'max_active_runs': 1,
    'queue': 'vod.remux'
}

dag = DAG(
    DAG_ID,
    default_args = default_args,
    start_date = START_DATE,
    schedule_interval = SCHEDULE_INTERVAL,
    catchup = False
)

with dag:
    s0 = PythonOperator(
            task_id='feed_providers',
            python_callable=feedProviders,
            provide_context=True,
            dag=dag,
    )
    s1 = PythonOperator(
            task_id='feed_items',
            python_callable=feedItems,
            provide_context=True,
            dag=dag,
    )
    s2 = PythonOperator(
            task_id='prepare_multiplexer',
            python_callable=prepareMux,
            provide_context=True,
            dag=dag,
    )

def mux(number, **kwargs):
    return PythonOperator(
        task_id='mux{}'.format(number),
        python_callable=doMux,
        provide_context=True,
        op_kwargs={'number': number},
        dag=dag,
    )

s0
s1 >> s2
for i in range(0,NO_OF_PARALLEL_CMDS):
    s2 >> mux(i)
