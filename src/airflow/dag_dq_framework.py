from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'dq-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dq_framework_daily',
    start_date=datetime(2025,7,1),
    schedule='0 3 * * *',
    catchup=False,
    default_args=default_args,
    tags=['dq','validation']
) as dag:
    run_validation = BashOperator(
        task_id='run_partition_validation',
        bash_command='python /opt/airflow/dags/src/run_partition_validation.py --date {{ ds }} --out reports'
    )

    run_validation
