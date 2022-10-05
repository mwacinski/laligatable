from datetime import datetime, date, timedelta
from scripts import script1
from airflow import DAG
import os
from airflow.operators.python import PythonOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_data = '/opt/airflow/data/'
today = str(date.today())
default_args = {
    'owner': 'airflow',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='laligadag_v02',
    start_date=datetime.today(),
    schedule_interval='@weekly'
) as dag:

    export_table = PythonOperator(
        dag=dag,
        task_id='export_table',
        op_kwargs={
            'today': today,
            'path': path_to_data
        },
        python_callable=script1.export_table
    )

    export_new_table = PythonOperator(
        dag=dag,
        task_id='export_new_table',
        op_kwargs={
            'today': today,
            'path': path_to_data
        },
        python_callable=script1.export_new_table
    )

    local_to_gcs = PythonOperator(
        task_id="local_to_gcs",
        python_callable=script1.upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{today}T.csv",
            "local_file": f"{path_to_data}{today}T.csv",
        },
    )

export_table >> export_new_table >> local_to_gcs