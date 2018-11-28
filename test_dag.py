import os
from datetime import datetime

from airflow.models import DAG
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators import dummy_operator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('test_dag',
          schedule_interval='@hourly',
          default_args=default_args,
          catchup=False
)

print(os.environ)

with dag:
    dummy_start = dummy_operator.DummyOperator(task_id='kickoff_dag')

    log_task = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='copy-to-gcs',
        name='copy-to-gcs',
        namespace='default',
        image_pull_policy='Always',
        image='gcr.io/dataeng-code/code:0.0.4',
        cmds=['python', 'dataeng_code/sqlserver/copy_to_gcs.py'],
        arguments=[]
    )

    dummy_start >> log_task
