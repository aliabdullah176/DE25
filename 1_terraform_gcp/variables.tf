variable "credentials" {
  description = "My Credentials"
  default     = "~/.gc/my_creds.json"
}

variable "project" {
    description = "project name"
    default = "de25-475501"
}

variable "region" {
    description = "project region"
    default = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "de25"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  # Update the below to a unique bucket name
  default     = "de25"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}