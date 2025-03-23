variable "project_id" {
  description = "GCP Project ID"
  type = string
  default = "de-zoomcamp-448805"
}

variable "region" {
  description = "GCP Region"
  default     = "us-west1"
}

variable "location" {
  description = "Location for the resources"
  type        = string
  default     = "US"
}

variable "gcs_storage_class" {
  description = "Storage class for the bucket"
  type        = string
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
  default     = "noaa-data-lake"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  type        = string
  default     = "noaa_dataset"
}
