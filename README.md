# Problem Statements 
The goal of this project is to build data pipelines that collect, process, and transform NOAA's yearly storm data into analytical insights. Some of the interesting questions that this project can help answer are: 
* What is the most common fatality type across different event types?
* How do fatalities vary by month?
* Which event types cause the most economic damage?
* What is the distribution of fatalities by sex?

# Dataset
The dataset used in this project is downloaded from the [NOAA's Storm Events Database](https://www.ncdc.noaa.gov/stormevents/). This dataset consists of 2 main types of files, one stores data about storm events with each record being an event, and the other stores data about fatalities, in which a row is a fatality case.
The relationship between 2 types of files is one-to-many in which one event can be linked to multiple fatality cases using an event_id field. Detailed information about the fields can be found here: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/Storm-Data-Bulk-csv-Format.pdf

# Technologies
* Cloud: GCP
* Infrastructure as code (IaC): Terraform
* Workflow orchestration: Airflow
* Data Warehouse: BigQuery
* Batch processing: Spark
# Architecture 
## Overview
Data in CSV format is initially downloaded from the NOAA website and stored in the local environment. This data is then loaded and processed using Pyspark before being uploaded to a GCS bucket in smaller partitions in parquet format. Subsequently, these files are then loaded into external tables before being copied to partitioned tables in BigQuery. The tables are partitioned by month to increase the query performance in the transformation step. This whole process is orchestrated and scheduled in Airflow. In the data transformation step, dbtCloud comes into play as it gets data from BigQuery and builds db models for a dashboard on Looker Studio.
## Architecture Diagram
![de_zoomcamp_project_architecture drawio](https://github.com/user-attachments/assets/0be5d2f4-07a1-459a-8b56-ef79fdcab3b3)

# Steps To Reproduce

### Prerequisites 
* Docker
* docker-compose
* Terraform

### Step 1: Clone the repository
```bash
git clone https://github.com/naivebird/noaa-storms-analysis.git
cd noaa-storms-analysis
```
### Step 2: Create a service account
Create a project and a service account on Google Cloud with these permissions:
* BigQuery Admin
* Storage Admin
* Storage Object Admin
* Viewer

Download the Service Account JSON file, rename it to `credentials.json` store it in `noaa-storms-analysis/.google/credentials/credentials.json`.

Also, make sure to activate these APIs:

* https://console.cloud.google.com/apis/library/iam.googleapis.com
* https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

### Step 3: Create cloud resources
Update the `project_id` variable in `variables.tf`

Init terraform:
```bash
terraform init
```
Check if resources are correct:
```bash
terraform plan
```
Create resources:
```bash
terraform apply
```

### Step 4: Start Airflow services
Open the `docker-compose.yml` file and update the following environment variable under the `x-airflow-common` service:
* GCS_BUCKET: "{project_id}_noaa-data-lake"
* GCP_PROJECT_ID: "{project_id}"

Create a `.env` file to store the required environment variables for Airflow:
```bash
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_PROJ_DIR=./ingestion" > .env
```
Start Airflow services:
```bash
docker-compose up --build -d
```
### Step 5: Trigger the ingestion pipeline
To access the Airflow webserver, go to this address: http://0.0.0.0:8080/home

Add connections for Spark and GCP by navigating to Admin -> Connections -> Add a new record

spark_default connection

![image](https://github.com/user-attachments/assets/c1308d71-51d7-4ae5-ba35-edb2cefa52e9)

google_cloud_default connection:

![image](https://github.com/user-attachments/assets/3109b053-1cd9-43b6-ac72-01df893d663f)


To ingest data to BigQuery, these 2 DAGS must be executed: `ingest_noaa_data` and `load_to_bq`.

You need to unpause both of them and manually trigger the first DAG, `ingest_noaa_data`, once it's complete, it will automatically trigger the second DAG to load data to BigQuery.

ingest_noaa_data DAG:

![image](https://github.com/user-attachments/assets/d54f1697-510a-4cc9-94c7-7f332438c597)

load_to_bq DAG:

![image](https://github.com/user-attachments/assets/ffca38db-33dd-4d95-ae2c-d50f1003925f)

After both DAGs are finished, you should see these tables in the `noaa_dataset` in your BigQuery:
* noaa_storms_2024_external
* noaa_fatalities_2024_external
* noaa_storms_partitioned
* noaa_fatalities_paritioned

External tables are created for each year but the partitioned tables include data for all the years. Both partitioned tables are partitioned by month for enhanced efficiency and performance because the analytical queries in this project tend to filter data by month the most.

The DAGs are scheduled to run yearly but you can backfill the data for the previous years by running this command:
```bash
 docker-compose exec airflow-scheduler airflow dags backfill -s 2020-01-01 -e 2024-01-01 ingest_noaa_data
```

### Step 6: Build data models
Create a new project on your dbtCloud, and connect it with the `noaa-storms-analysis/dbt` subdirectory in this project. Connect the dbt project with your BigQuery db, and change the location setting to `us-west1`.

Update the database name in `staging/schema.yml` with your project ID and build the models by running:

```bash
dbt build
```

Linage:

![image](https://github.com/user-attachments/assets/30ebb7e1-b0b0-456b-a4bf-67d4a13b0115)


The built models can be found in the `dbt_noaa_storms_data` dataset in your BigQuery database.

### Step 7: Create a dashboard
Create a new report in Looker Studio and connect it with the database models in your BigQuery to create charts that help you answer analytical questions.

Here is my dashboard:

![image](https://github.com/user-attachments/assets/eb5bb0ae-91e5-42a9-9a1a-66f3fd3d99b6)

The project dashboard can be found here: https://lookerstudio.google.com/s/phPXVszs9fE

Insights for the year 2024 from the dashboard:
* Heat-related events caused the most deaths with a relatively balanced distribution of direct and indirect deaths.
* Most of the deaths caused by fast floods, rip currents, and tornado events are direct.
* June had the most deaths by natural events.
* Hurricane caused the most economic damage with a total cost of nearly 7 billion dollars.
* As always, while men account for 72% of deaths, women only make up 25.5%, with the remaining cases classified as unknown.

### Step 8: Destroy the cloud resources
Don't forget to remove the GCP resources you created earlier:
```bash
terraform destroy
```
