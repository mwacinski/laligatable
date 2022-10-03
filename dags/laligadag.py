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

    export_table = PythonOperator(
        dag=dag,
        task_id='export_table',
        op_kwargs={
            'today': str(date.today()),
            'path':'/opt/airflow/data/'
        },
        python_callable=script1.export_table
    )

    export_new_table = PythonOperator(
        dag=dag,
        task_id='export_new_table',
        op_kwargs={
            'today': str(date.today()),
            'path': '/opt/airflow/data/'
        },
        python_callable=script1.export_new_table
    )


export_table >> export_new_table