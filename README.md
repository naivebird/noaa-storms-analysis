# Problem Statements 
The goal of this project is to build data pipelines that collect, process, and transform NOAA's storm data into analytical insights. Some of the interesting questions that this project can help answer are: 
* What is the most common fatality type across different event types?
* How do fatalities vary by month?
* Which event types cause the most economic damage?
* What is the distribution of fatalities by sex?

# Dataset
The dataset being used in this project is downloaded from the [NOAA's Storm Events Database](https://www.ncdc.noaa.gov/stormevents/). This dataset consists of 2 main types of files, one stores data about storm events with each record being an event, and the other stores data about fatalities, in which a row is a fatality case.
The relationship between 2 types of files is one-to-many with one event can be linked to multiple fatality cases using an event_id field. Detailed information about the fields can be found here: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/Storm-Data-Bulk-csv-Format.pdf

# Technologies
* Cloud: GCP
* Infrastructure as code (IaC): Terraform
* Workflow orchestration: Airflow
* Data Warehouse: BigQuery
* Batch processing: Spark
# Architecture 
## Overview
Data in CSV format is initially downloaded from the NOAA website and stored in the local environment. This data is then loaded and processed using Pyspark before being uploaded to a GCS bucket in smaller partitions in parquet format. Subsequently, these files are then loaded into external tables before being copied to partitioned tables in BigQuery. The tables are partitioned by month to increase the query performance in the transformation step. This whole process is orchestrated and scheduled in Airflow. In the data transformation step, dbtCloud comes into play as it gets data from BigQuery and builds db models for a dashboard on Looker Studio.
## Diagram
### Project architecture
![de_zoomcamp_project_architecture drawio](https://github.com/user-attachments/assets/0be5d2f4-07a1-459a-8b56-ef79fdcab3b3)

### dbtCloud lineages
![image](https://github.com/user-attachments/assets/b4f30379-c1fb-46bd-930b-5d22ca25d8b8)
![image](https://github.com/user-attachments/assets/7b6d8881-0214-4ec2-adb0-fac8f92c3669)

# Dashboard
![image](https://github.com/user-attachments/assets/001916a1-9466-4c88-9a19-757cfd752d8c)

The project dashboard can be found here: https://lookerstudio.google.com/s/phPXVszs9fE

# Steps To Reproduce

### Prerequisites 
* Docker
* docker-compose
* terraform

### Step 1
Clone the repository:
```bash
git clone https://github.com/naivebird/noaa-storms-analysis.git
```
### Step 2
Create a project and a service account on Google Cloud with these permissions:
* BigQuery Admin
* Storage Admin
* Storage Object Admin
* Viewer
Download the Service Account JSON file, rename it to credentials.json store it in `noaa-storms-analysis/.google/credentials/credentials.json`.

Also, make sure to activate these APIs:

https://console.cloud.google.com/apis/library/iam.googleapis.com
https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

### Step 3
Update the project_id variable in `variables.tf`
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

### Step 4
Open the docker-compose.yml file and update the following environment variable under the x-airflow-common service:
* GCS_BUCKET: "de-zoomcamp-448805_noaa-data-lake"
* BQ_DATASET: "noaa_dataset"
* GCP_PROJECT_ID: "de-zoomcamp-448805"

