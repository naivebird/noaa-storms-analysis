import os
import shutil
from datetime import datetime
import glob

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from scripts.download_noaa_data import download_noaa_data

year = Variable.get("year", 2024)

gcs_bucket = os.getenv("GCS_BUCKET")

RAW_STORMS_FILE = f"/tmp/noaa_storms_{year}.csv.gz"
PROCESSED_STORMS_FILE = f"/tmp/noaa_storms_{year}.parquet"

RAW_FATALITIES_FILE = f"/tmp/noaa_fatalities_{year}.csv.gz"
PROCESSED_FATALITIES_FILE = f"/tmp/noaa_fatalities_{year}.parquet"

BUCKET_STORMS_PATH = f"noaa_storms_{year}.parquet"
BUCKET_FATALITIES_PATH = f"noaa_fatalities_{year}.parquet"


def download_data():
    download_noaa_data(year, "details", RAW_STORMS_FILE)
    download_noaa_data(year, "fatalities", RAW_FATALITIES_FILE)


def clean_up():
    os.remove(RAW_STORMS_FILE)
    os.remove(RAW_FATALITIES_FILE)
    shutil.rmtree(PROCESSED_STORMS_FILE)
    shutil.rmtree(PROCESSED_FATALITIES_FILE)


default_args = {"owner": "ingestion", "start_date": datetime(2024, 3, 21)}

with DAG("ingest_noaa_data", default_args=default_args, schedule="@yearly", catchup=False) as dag:
    download_data_task = PythonOperator(
        task_id="download_noaa_data",
        python_callable=download_data,
    )

    process_storms_data_task = SparkSubmitOperator(
        task_id="process_storms_data",
        application="/opt/airflow/dags/scripts/process_noaa_data.py",
        conn_id="spark_default",
        conf={"spark.master": "local[*]"},
        application_args=[RAW_STORMS_FILE, PROCESSED_STORMS_FILE],
        dag=dag
    )

    process_fatalities_data_task = SparkSubmitOperator(
        task_id="process_fatalities_data",
        application="/opt/airflow/dags/scripts/process_noaa_data.py",
        conn_id="spark_default",
        conf={"spark.master": "local[*]"},
        application_args=[RAW_FATALITIES_FILE, PROCESSED_FATALITIES_FILE],
        dag=dag
    )

    upload_storms_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_storms_to_gcs",
        gcp_conn_id="google_cloud_default",
        bucket=gcs_bucket,
        src=glob.glob(PROCESSED_STORMS_FILE + "/*.parquet"),
        dst=f"noaa_storms_{year}/",
        mime_type="application/octet-stream",
    )

    upload_fatalities_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_fatalities_to_gcs",
        gcp_conn_id="google_cloud_default",
        bucket=gcs_bucket,
        src=glob.glob(PROCESSED_FATALITIES_FILE + "/*.parquet"),
        dst=f"noaa_fatalities_{year}/",
        mime_type="application/octet-stream",
    )

    remove_local_files_task = PythonOperator(
        task_id="remove_local_files",
        python_callable=clean_up
    )

    download_data_task >> [process_storms_data_task, process_fatalities_data_task]
    process_storms_data_task >> upload_storms_to_gcs
    process_fatalities_data_task >> upload_fatalities_to_gcs
    [upload_storms_to_gcs, upload_fatalities_to_gcs] >> remove_local_files_task
