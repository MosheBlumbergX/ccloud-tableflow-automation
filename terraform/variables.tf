variable "prefix" {
  description = "Prefix applied to every created resource name so several people can share an organization and subscription"
  type        = string
  default     = "moshe"

  validation {
    condition     = can(regex("^[a-z0-9-]{2,20}$", var.prefix))
    error_message = "Must be 2-20 lowercase alphanumeric characters or hyphens."
  }
}

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key with permission to create environments"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret"
  type        = string
  sensitive   = true
}

variable "kafka_cluster_region" {
  description = "Azure region for the Kafka cluster. Must support Tableflow and match the storage account region."
  type        = string
  default     = "eastus2"
}

variable "kafka_cluster_availability" {
  description = "SINGLE_ZONE or MULTI_ZONE"
  type        = string
  default     = "SINGLE_ZONE"
}

variable "customer_azure_tenant_id" {
  description = "Azure tenant ID that the Confluent provider integration is authorized against"
  type        = string
}

variable "azure_subscription_id" {
  description = "Azure subscription that holds the ADLS Gen2 account"
  type        = string
}

variable "storage_account_name" {
  # Leave this null. Reusing a storage account name shortly after destroying it
  # produces a stack where Tableflow writes correctly but readers cannot fetch
  # the parquet files, so the default generates a fresh name on every rebuild.
  description = "Override the generated ADLS Gen2 account name (3-24 lowercase alphanumeric, globally unique, not recently used)"
  type        = string
  default     = null

  validation {
    condition     = var.storage_account_name == null || can(regex("^[a-z0-9]{3,24}$", coalesce(var.storage_account_name, "placeholder")))
    error_message = "Must be 3-24 lowercase alphanumeric characters."
  }
}

variable "storage_container_name" {
  description = "ADLS Gen2 container that Tableflow writes Iceberg data into"
  type        = string
  default     = "tableflow-data"
}
