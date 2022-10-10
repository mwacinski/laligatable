from datetime import datetime, date, timedelta
from scripts import etlprocess
from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
INPUT_PART = "data"
INPUT_FILETYPE = "csv"
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

    export_table_matches = PythonOperator(
        dag=dag,
        task_id='export_table_matches',
        op_kwargs={
            'today': today,
            'path': path_to_data
        },
        python_callable=etlprocess.export_table_matches
    )

    export_points_table = PythonOperator(
        dag=dag,
        task_id='export_points_table',
        op_kwargs={
            'today': today,
            'path': path_to_data
        },
        python_callable=etlprocess.export_points_table
    )

    local_to_gcs_table_matches = PythonOperator(
        task_id="local_to_gcs_table_matches",
        python_callable=etlprocess.upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"data/{today}.csv",
            "local_file": f"{path_to_data}{today}.csv",
        },
    )

    local_to_gcs_points_table = PythonOperator(
        task_id="local_to_gcs_points_table",
        python_callable=etlprocess.upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"data/{today}T.csv",
            "local_file": f"{path_to_data}{today}T.csv",
        },
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=BIGQUERY_DATASET,
        location="europe-central2"
    )

    bigquery_table_matches_task = GCSToBigQueryOperator(
        task_id=f"bq_{BIGQUERY_DATASET}_table_matches",
        bucket=BUCKET,
        source_objects=[f"data/{today}.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_DATASET}_table_matches",
        schema_fields=[
            {"name": "Title_of_Away_Team", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Short_Title_of_Away_Team", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Title_of_Home_Team", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Short_Title_of_Home_Team", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DateTime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "Chances_for_draw", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Chances_for_away_team", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Chances_for_home_team", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Away_team_goals", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Home_team_goals", "type": "INTEGER", "mode": "NULLABLE"}

        ],
        source_format=f"{INPUT_FILETYPE.upper()}",
        write_disposition='WRITE_TRUNCATE',
        location='europe-central2',
        skip_leading_rows=1
    )
    bigquery_points_table_task = GCSToBigQueryOperator(
        task_id=f"bq_{BIGQUERY_DATASET}_points_table",
        bucket=BUCKET,
        source_objects=[f"data/{today}T.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_DATASET}_points_table",
        schema_fields=[
            {"name": "teams", "type": "STRING", "mode": "NULLABLE"},
            {"name": "goals", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "goals_lost", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "games_played", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "games_won", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "games_draw", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "games_lost", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "goal_balance", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "points", "type": "INTEGER", "mode": "NULLABLE"}

        ],
        source_format=f"{INPUT_FILETYPE.upper()}",
        write_disposition='WRITE_TRUNCATE',
        location='europe-central2',
        skip_leading_rows=1

    )

export_table_matches >> export_points_table >> local_to_gcs_table_matches >> local_to_gcs_points_table >> create_dataset >> bigquery_table_matches_task  >> bigquery_points_table_task