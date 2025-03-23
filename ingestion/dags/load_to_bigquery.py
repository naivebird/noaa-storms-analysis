import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from datetime import datetime

year = Variable.get("year", 2024)

gcs_bucket = os.getenv("GCS_BUCKET")
bq_dataset = os.getenv("BQ_DATASET")
project_id = os.getenv("GCP_PROJECT_ID")

BQ_EXTERNAL_STORMS_TABLE = f"noaa_storms_{year}_external"
BQ_EXTERNAL_FATALITIES_TABLE = f"noaa_fatalities_{year}_external"
BQ_PARTITIONED_STORMS_TABLE = "noaa_storms_partitioned"
BQ_PARTITIONED_FATALITIES_TABLE = "noaa_fatalities_partitioned"

default_args = {"owner": "ingestion", "start_date": datetime(2024, 3, 21)}

with DAG("load_to_bq", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:

    create_storms_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_storms_table",
        table_resource={
            "tableReference": {"projectId": project_id, "datasetId": bq_dataset, "tableId": BQ_EXTERNAL_STORMS_TABLE},
            "externalDataConfiguration": {
                "sourceUris": [f"gs://{gcs_bucket}/noaa_storms_{year}/*.parquet"],
                "sourceFormat": "PARQUET",
            }
        }
    )
    create_partitioned_storms_table = BigQueryCreateEmptyTableOperator(
        task_id="create_partitioned_storms_table",
        dataset_id=bq_dataset,
        table_id=BQ_PARTITIONED_STORMS_TABLE,
        project_id=project_id,
        exists_ok=True,
        time_partitioning={"type": "MONTH", "field": "event_start_date"},
        schema_fields=[
            {"name": "event_id", "type": "STRING"},
            {"name": "state", "type": "STRING"},
            {"name": "event_type", "type": "STRING"},
            {"name": "event_start_date", "type": "TIMESTAMP"},
            {"name": "event_end_date", "type": "TIMESTAMP"},
            {"name": "injuries_direct", "type": "INTEGER"},
            {"name": "injuries_indirect", "type": "INTEGER"},
            {"name": "deaths_direct", "type": "INTEGER"},
            {"name": "deaths_indirect", "type": "INTEGER"},
            {"name": "damage_property", "type": "INTEGER"},
            {"name": "damage_crops", "type": "INTEGER"},
            {"name": "source", "type": "STRING"},
            {"name": "magnitude", "type": "FLOAT"},
            {"name": "magnitude_type", "type": "STRING"},
        ],
    )

    copy_to_partitioned_storms_table = BigQueryInsertJobOperator(
        task_id="copy_to_partitioned_storms_table",
        configuration={
            "query": {
                "query": f"""
                        INSERT INTO `{bq_dataset}.{BQ_PARTITIONED_STORMS_TABLE}`
                        SELECT * FROM `{bq_dataset}.{BQ_EXTERNAL_STORMS_TABLE}`;
                    """,
                "useLegacySql": False,
            }
        }
    )

    create_fatalities_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_fatalities_table",
        table_resource={
            "tableReference": {"projectId": project_id, "datasetId": bq_dataset, "tableId": BQ_EXTERNAL_FATALITIES_TABLE},
            "externalDataConfiguration": {
                "sourceUris": [f"gs://{gcs_bucket}/noaa_fatalities_{year}/*.parquet"],
                "sourceFormat": "PARQUET",
            },
        },
    )
    create_partitioned_fatalities_table = BigQueryCreateEmptyTableOperator(
        task_id="create_partitioned_fatalities_table",
        dataset_id=bq_dataset,
        table_id=BQ_PARTITIONED_FATALITIES_TABLE,
        project_id=project_id,
        exists_ok=True,
        time_partitioning={"type": "MONTH", "field": "fatality_date"},
        schema_fields=[
            {"name": "fatality_id", "type": "STRING"},
            {"name": "event_id", "type": "STRING"},
            {"name": "fatality_type", "type": "STRING"},
            {"name": "fatality_date", "type": "TIMESTAMP"},
            {"name": "fatality_age", "type": "INTEGER"},
            {"name": "fatality_sex", "type": "STRING"},
            {"name": "fatality_location", "type": "STRING"}
        ],
    )

    copy_to_partitioned_fatalities_table = BigQueryInsertJobOperator(
        task_id="copy_to_partitioned_fatalities_table",
        configuration={
            "query": {
                "query": f"""
                            INSERT INTO `{bq_dataset}.{BQ_PARTITIONED_FATALITIES_TABLE}`
                            SELECT * FROM `{bq_dataset}.{BQ_EXTERNAL_FATALITIES_TABLE}`;
                        """,
                "useLegacySql": False,
            }
        }
    )

    create_storms_external_table >> create_partitioned_storms_table >> copy_to_partitioned_storms_table
    create_fatalities_external_table >> create_partitioned_fatalities_table >> copy_to_partitioned_fatalities_table



