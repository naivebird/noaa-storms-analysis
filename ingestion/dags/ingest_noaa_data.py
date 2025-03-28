import os
import shutil
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from scripts.download_noaa_data import download_noaa_data

gcs_bucket = os.getenv("GCS_BUCKET")


def download_data(execution_date, **context):
    year = execution_date.year - 1
    download_noaa_data(year, "details", f"/tmp/noaa_storms_{year}.csv.gz")
    download_noaa_data(year, "fatalities", f"/tmp/noaa_fatalities_{year}.csv.gz")


def clean_up(execution_date, **context):
    year = execution_date.year - 1
    os.remove(f"/tmp/noaa_storms_{year}.csv.gz")
    os.remove(f"/tmp/noaa_fatalities_{year}.csv.gz")
    shutil.rmtree(f"/tmp/noaa_storms_{year}.parquet")
    shutil.rmtree(f"/tmp/noaa_fatalities_{year}.parquet")


default_args = {"owner": "ingestion", "start_date": datetime(1951, 1, 1)}

with DAG("ingest_noaa_data", default_args=default_args, schedule="@yearly", catchup=False) as dag:
    download_data_task = PythonOperator(
        task_id="download_noaa_data",
        python_callable=download_data,
        provide_context=True
    )

    process_storms_data_task = SparkSubmitOperator(
        task_id="process_storms_data",
        application="/opt/airflow/dags/scripts/process_noaa_data.py",
        conn_id="spark_default",
        conf={"spark.master": "local[*]"},
        application_args=[
            "/tmp/noaa_storms_{{ execution_date.year - 1 }}.csv.gz",
            "/tmp/noaa_storms_{{ execution_date.year - 1 }}.parquet"
        ],
        dag=dag
    )

    process_fatalities_data_task = SparkSubmitOperator(
        task_id="process_fatalities_data",
        application="/opt/airflow/dags/scripts/process_noaa_data.py",
        conn_id="spark_default",
        conf={"spark.master": "local[*]"},
        application_args=[
            "/tmp/noaa_fatalities_{{ execution_date.year - 1 }}.csv.gz",
            "/tmp/noaa_fatalities_{{ execution_date.year - 1 }}.parquet"
        ],
        dag=dag
    )

    upload_storms_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_storms_to_gcs",
        gcp_conn_id="google_cloud_default",
        bucket=gcs_bucket,
        src="/tmp/noaa_storms_{{ execution_date.year - 1 }}.parquet/*.parquet",
        dst="noaa_storms_{{ execution_date.year - 1 }}/",
        mime_type="application/octet-stream",
    )

    upload_fatalities_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_fatalities_to_gcs",
        gcp_conn_id="google_cloud_default",
        bucket=gcs_bucket,
        src="/tmp/noaa_fatalities_{{ execution_date.year - 1 }}.parquet/*.parquet",
        dst="noaa_fatalities_{{ execution_date.year - 1 }}/",
        mime_type="application/octet-stream",
    )

    remove_local_files_task = PythonOperator(
        task_id="remove_local_files",
        python_callable=clean_up,
        provide_context=True
    )

    trigger_load_to_bq = TriggerDagRunOperator(
        task_id="trigger_load_to_bq",
        trigger_dag_id="load_to_bq",
        conf={"year": "{{ execution_date.year - 1 }}"}
    )

    download_data_task >> [process_storms_data_task, process_fatalities_data_task]
    process_storms_data_task >> upload_storms_to_gcs
    process_fatalities_data_task >> upload_fatalities_to_gcs
    [upload_storms_to_gcs, upload_fatalities_to_gcs] >> remove_local_files_task >> trigger_load_to_bq
