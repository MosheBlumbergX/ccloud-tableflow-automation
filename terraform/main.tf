terraform {
  required_version = ">= 1.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.57"
    }
    azuread = {
      source = "hashicorp/azuread"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
}

provider "azuread" {
  tenant_id = var.customer_azure_tenant_id
}

locals {
  # Tableflow reads data files straight out of ADLS, so the storage account has
  # to live in the same region as the Kafka cluster.
  region = var.kafka_cluster_region

  # A storage account name may not be reused shortly after being destroyed:
  # Tableflow keeps writing but readers fail to fetch the parquet files. A
  # random suffix regenerates on every apply that follows a destroy, so each
  # rebuild gets a fresh name.
  storage_account_name = coalesce(
    var.storage_account_name,
    "${replace(var.prefix, "-", "")}tf${random_id.storage_suffix.hex}",
  )
}

resource "random_id" "storage_suffix" {
  byte_length = 4
}

# --- Confluent Cloud environment and cluster ----------------------------------
resource "confluent_environment" "main" {
  display_name = "${var.prefix}-tableflow-demo"

  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "main" {
  display_name = "${var.prefix}-tableflow-cluster"
  availability = var.kafka_cluster_availability
  cloud        = "AZURE"
  region       = local.region

  standard {}

  environment {
    id = confluent_environment.main.id
  }
}

data "confluent_schema_registry_cluster" "essentials" {
  environment {
    id = confluent_environment.main.id
  }

  depends_on = [confluent_kafka_cluster.main]
}

# --- Azure provider integration -------------------------------------------------
resource "confluent_provider_integration_setup" "azure" {
  environment {
    id = confluent_environment.main.id
  }

  display_name = "${var.prefix}-azure-integration"
  cloud        = "AZURE"
}

resource "confluent_provider_integration_authorization" "azure" {
  provider_integration_id = confluent_provider_integration_setup.azure.id

  environment {
    id = confluent_environment.main.id
  }

  azure {
    customer_azure_tenant_id = var.customer_azure_tenant_id
  }
}

# The multi-tenant Confluent application needs a service principal in this
# tenant before role assignments can reference it, and it is not created
# automatically the first time the tenant sees the app.
resource "null_resource" "confluent_azure_sp" {
  triggers = {
    app_id = confluent_provider_integration_authorization.azure.azure[0].confluent_multi_tenant_app_id
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -euo pipefail
      APP_ID="${self.triggers.app_id}"
      for _ in 1 2 3 4 5 6; do
        if az ad sp show --id "$APP_ID" --query id -o tsv >/dev/null 2>&1; then
          exit 0
        fi
        az ad sp create --id "$APP_ID" && exit 0 || true
        sleep 10
      done
      az ad sp show --id "$APP_ID" --query id -o tsv
    EOT
  }

  depends_on = [confluent_provider_integration_authorization.azure]
}

data "azuread_service_principal" "confluent" {
  client_id = confluent_provider_integration_authorization.azure.azure[0].confluent_multi_tenant_app_id

  depends_on = [null_resource.confluent_azure_sp]
}

# --- Azure Data Lake Storage Gen2 ------------------------------------------------
resource "azurerm_resource_group" "confluent" {
  name     = "${var.prefix}-tableflow-rg"
  location = local.region
}

resource "azurerm_storage_account" "adls" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.confluent.name
  location                 = azurerm_resource_group.confluent.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  # ADLS Gen2 requires a hierarchical namespace.
  is_hns_enabled = true
}

resource "azurerm_storage_container" "tableflow" {
  name                  = var.storage_container_name
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_role_assignment" "blob_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_service_principal.confluent.object_id
}

resource "azurerm_role_assignment" "reader" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Reader"
  principal_id         = data.azuread_service_principal.confluent.object_id
}

# --- Service accounts and API keys ------------------------------------------------
# app-manager backs the CFK KafkaTopic and Schema custom resources.
resource "confluent_service_account" "app_manager" {
  display_name = "${var.prefix}-tableflow-app-manager"
  description  = "Used by CFK to manage Kafka topics and schemas in Confluent Cloud"
}

resource "confluent_role_binding" "app_manager_cluster_admin" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

resource "confluent_role_binding" "app_manager_environment_admin" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

resource "confluent_role_binding" "app_manager_subject_owner" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.essentials.resource_name}/subject=*"
}

resource "confluent_api_key" "app_manager_kafka" {
  display_name = "${var.prefix}-tableflow-kafka-api-key"
  description  = "Kafka API key used by the CFK KafkaTopic custom resources"

  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [confluent_role_binding.app_manager_cluster_admin]
}

resource "confluent_api_key" "app_manager_schema_registry" {
  display_name = "${var.prefix}-tableflow-sr-api-key"
  description  = "Schema Registry API key used by the CFK Schema custom resources and the watcher"

  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.essentials.id
    api_version = data.confluent_schema_registry_cluster.essentials.api_version
    kind        = data.confluent_schema_registry_cluster.essentials.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [
    confluent_role_binding.app_manager_environment_admin,
    confluent_role_binding.app_manager_subject_owner,
  ]
}

# The Tableflow API rejects ordinary Cloud API keys, so the watcher needs a key
# whose managed resource is "tableflow".
resource "confluent_service_account" "tableflow" {
  display_name = "${var.prefix}-tableflow-sa"
  description  = "Owns the Tableflow API key used by the in-cluster watcher"
}

resource "confluent_role_binding" "tableflow_cluster_admin" {
  principal   = "User:${confluent_service_account.tableflow.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn
}

resource "confluent_role_binding" "tableflow_environment_admin" {
  principal   = "User:${confluent_service_account.tableflow.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

resource "confluent_role_binding" "tableflow_subject_read" {
  principal   = "User:${confluent_service_account.tableflow.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.essentials.resource_name}/subject=*"
}

resource "confluent_api_key" "tableflow" {
  display_name = "${var.prefix}-tableflow-api-key"
  description  = "Tableflow API key used by the in-cluster watcher"

  owner {
    id          = confluent_service_account.tableflow.id
    api_version = confluent_service_account.tableflow.api_version
    kind        = confluent_service_account.tableflow.kind
  }

  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"

    environment {
      id = confluent_environment.main.id
    }
  }

  depends_on = [
    confluent_role_binding.tableflow_cluster_admin,
    confluent_role_binding.tableflow_environment_admin,
    confluent_role_binding.tableflow_subject_read,
  ]
}
