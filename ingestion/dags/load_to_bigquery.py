import os
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator

gcs_bucket = os.getenv("GCS_BUCKET")
bq_dataset = os.getenv("BQ_DATASET")
project_id = os.getenv("GCP_PROJECT_ID")

BQ_PARTITIONED_STORMS_TABLE = "noaa_storms_partitioned"
BQ_PARTITIONED_FATALITIES_TABLE = "noaa_fatalities_partitioned"

default_args = {"owner": "ingestion", "start_date": datetime(1951, 1, 1)}

with DAG("load_to_bq", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    start = EmptyOperator(
        task_id="load_to_bq"
    )
    create_external_storms_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_storms_table",
        table_resource={
            "tableReference": {
                "projectId": project_id,
                "datasetId": bq_dataset,
                "tableId": "noaa_storms_{{ dag_run.conf['year'] }}_external"
            },
            "externalDataConfiguration": {
                "sourceUris": [
                    "gs://{gcs_bucket}/noaa_storms_{year}/*.parquet".format(
                        gcs_bucket=gcs_bucket,
                        year="{{ dag_run.conf['year'] }}"
                    )
                ],
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
                "query": """
                        INSERT INTO `{bq_dataset}.{partitioned_storms_table}`
                        SELECT * FROM `{bq_dataset}.{external_storms_table}`;
                    """.format(
                    bq_dataset=bq_dataset,
                    partitioned_storms_table=BQ_PARTITIONED_STORMS_TABLE,
                    external_storms_table="noaa_storms_{{ dag_run.conf['year'] }}_external"),
                "useLegacySql": False,
            }
        }
    )

    create_external_fatalities_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_fatalities_table",
        table_resource={
            "tableReference": {"projectId": project_id, "datasetId": bq_dataset,
                               "tableId": "noaa_fatalities_{{ dag_run.conf['year'] }}_external"},
            "externalDataConfiguration": {
                "sourceUris": [
                    "gs://{gcs_bucket}/noaa_fatalities_{year}/*.parquet".format(
                        gcs_bucket=gcs_bucket,
                        year="{{ dag_run.conf['year'] }}"
                    )
                ],
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
                "query": """
                            INSERT INTO `{bq_dataset}.{partitioned_fatalities_table}`
                            SELECT * FROM `{bq_dataset}.{external_fatalities_table}`;
                        """.format(
                    bq_dataset=bq_dataset,
                    partitioned_fatalities_table=BQ_PARTITIONED_FATALITIES_TABLE,
                    external_fatalities_table="noaa_fatalities_{{ dag_run.conf['year'] }}_external"
                ),
                "useLegacySql": False,
            }
        }
    )

    start >> [create_external_storms_table, create_external_fatalities_table]
    create_external_storms_table >> create_partitioned_storms_table >> copy_to_partitioned_storms_table
    create_external_fatalities_table >> create_partitioned_fatalities_table >> copy_to_partitioned_fatalities_table
