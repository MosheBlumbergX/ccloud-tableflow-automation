# deploy.sh consumes these with `terraform output -json`, so keep them flat and
# machine-readable rather than formatting them into a single report string.

output "prefix" {
  description = "Prefix applied to every created resource"
  value       = var.prefix
}

output "environment_id" {
  description = "Confluent Cloud environment ID"
  value       = confluent_environment.main.id
}

output "kafka_cluster_id" {
  description = "Kafka cluster ID"
  value       = confluent_kafka_cluster.main.id
}

output "kafka_rest_endpoint" {
  description = "Kafka Admin REST endpoint used by the CFK KafkaTopic custom resources"
  value       = confluent_kafka_cluster.main.rest_endpoint
}

output "kafka_bootstrap_endpoint" {
  description = "Bootstrap servers for producers and consumers"
  value       = confluent_kafka_cluster.main.bootstrap_endpoint
}

output "schema_registry_id" {
  description = "Schema Registry cluster ID"
  value       = data.confluent_schema_registry_cluster.essentials.id
}

output "schema_registry_endpoint" {
  description = "Schema Registry REST endpoint"
  value       = data.confluent_schema_registry_cluster.essentials.rest_endpoint
}

output "provider_integration_id" {
  description = "Azure provider integration ID that Tableflow uses to reach ADLS Gen2"
  value       = confluent_provider_integration_authorization.azure.id
}

output "storage_account_name" {
  description = "ADLS Gen2 account holding the Iceberg data"
  value       = azurerm_storage_account.adls.name
}

output "storage_container_name" {
  description = "ADLS Gen2 container holding the Iceberg data"
  value       = azurerm_storage_container.tableflow.name
}

output "resource_group_name" {
  description = "Azure resource group holding the storage account"
  value       = azurerm_resource_group.confluent.name
}

output "kafka_api_key" {
  description = "Kafka API key for the CFK KafkaTopic custom resources"
  value       = confluent_api_key.app_manager_kafka.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Kafka API secret for the CFK KafkaTopic custom resources"
  value       = confluent_api_key.app_manager_kafka.secret
  sensitive   = true
}

output "schema_registry_api_key" {
  description = "Schema Registry API key for the CFK Schema custom resources and the watcher"
  value       = confluent_api_key.app_manager_schema_registry.id
  sensitive   = true
}

output "schema_registry_api_secret" {
  description = "Schema Registry API secret"
  value       = confluent_api_key.app_manager_schema_registry.secret
  sensitive   = true
}

output "tableflow_api_key" {
  description = "Tableflow API key for the watcher"
  value       = confluent_api_key.tableflow.id
  sensitive   = true
}

output "tableflow_api_secret" {
  description = "Tableflow API secret for the watcher"
  value       = confluent_api_key.tableflow.secret
  sensitive   = true
}
