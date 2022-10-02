from datetime import datetime, date, timedelta
from scripts import script1
from airflow import DAG
from airflow.operators.python import PythonOperator

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

    export_df = PythonOperator(
        dag=dag,
        task_id='export_df',
        op_kwargs={
            'today': str(date.today()),
            'path':'/opt/airflow/data/'
        },
        python_callable=script1.export_df
    )

    transform = PythonOperator(
        dag=dag,
        task_id='transform',
        op_kwargs={
            'today': str(date.today()),
            'path': '/opt/airflow/data/'
        },
        python_callable=script1.transform_data
    )


export_df >> transform