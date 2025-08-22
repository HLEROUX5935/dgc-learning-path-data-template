variable "project_id" {
  type        = string
  description = "Project identifier"
   default = "vast-verve-469412-c5"
}

variable "pubsub_topic_id"{
  description = "GCP location"
  type = string
  default = "valid_file"
}

#Pour Workflows, il faut toujours une région spécifique (ex. europe-west1).
#Pour BigQuery et Cloud Storage, tu peux avoir une multi-région (EU, US) ou une région (europe-west1).
variable "region"{
  description = "GCP region"
  type = string
  default = "europe-west1"
}

variable "location"{
  description = "GCP location"
  type = string
  default = "EU"
}


variable "bucket_location" {
  type = string
  default = "us-east1  pas sur"
}

variable "python_code_location" {
  type = string
  default = "../cloud_functions/cf_trigger_on_file/src"
}

variable "cleaned_store_sql" {
  type = string
  default = "../queries/cleaned/store.sql"
}
variable "raw_store_json" {
  type = string
  default = "../schemas/raw/store.json"
}
variable "cleaned_store_json" {
  type = string
  default = "../schemas/cleaned/store.json"
}