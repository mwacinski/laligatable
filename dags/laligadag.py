from datetime import datetime, date, timedelta
from scripts import etlprocess
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
        python_callable=etlprocess.export_table
    )

    export_new_table = PythonOperator(
        dag=dag,
        task_id='export_new_table',
        op_kwargs={
            'today': today,
            'path': path_to_data
        },
        python_callable=etlprocess.export_new_table
    )

    local_to_gcs_table = PythonOperator(
        task_id="local_to_gcs_table",
        python_callable=etlprocess.upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"data/{today}.csv",
            "local_file": f"{path_to_data}{today}.csv",
        },
    )

    local_to_gcs_transformed_table = PythonOperator(
        task_id="local_to_gcs_transformed_table",
        python_callable=etlprocess.upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"data/{today}T.csv",
            "local_file": f"{path_to_data}{today}T.csv",
        },
    )

export_table >> export_new_table >> [local_to_gcs_table, local_to_gcs_transformed_table]