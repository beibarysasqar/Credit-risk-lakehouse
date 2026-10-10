# Terraform owns catalogs, schemas, volumes and grants; bundles own pipelines and jobs.

locals {
  schemas = {
    raw     = "Landing zone: volumes with source files as delivered"
    bronze  = "Raw ingested tables, source schema plus ingestion metadata"
    silver  = "Cleaned, typed and deduplicated tables"
    gold    = "Business-level marts (client_month)"
    history = "SCD2 history base and model-validation snapshots"
    # Job-owned tables that are not part of the medallion flow.
    monitoring = "Data-quality reporting: expectation results per pipeline update"
    sandbox    = "Experiments: layout benchmark copies and the COPY INTO demo table"
  }

  # One state per environment: Terraform workspace -> catalog it may manage.
  workspace_catalogs = {
    default = "credit_risk_dev"
    prod    = "credit_risk_prod"
  }
}

# Free Edition uses Default Storage, so the catalog cannot be created through the REST API
# (and therefore not by this provider). It is created once with CREATE CATALOG on the
# serverless warehouse and adopted here.
import {
  to = databricks_catalog.this
  id = var.catalog
}

resource "databricks_catalog" "this" {
  name    = var.catalog
  comment = "Credit risk lakehouse catalog"

  lifecycle {
    # Applying the tfvars of one environment on the state of the other would replace its schemas.
    precondition {
      condition     = lookup(local.workspace_catalogs, terraform.workspace, null) == var.catalog
      error_message = "Catalog ${var.catalog} does not belong to Terraform workspace ${terraform.workspace}."
    }
    prevent_destroy = true
    # Assigned by Default Storage / the metastore, not managed here.
    ignore_changes = [
      storage_root,
      properties,
      owner,
      isolation_mode,
      enable_predictive_optimization,
    ]
  }
}

resource "databricks_schema" "this" {
  for_each = local.schemas

  catalog_name = databricks_catalog.this.name
  name         = each.key
  comment      = each.value
}

resource "databricks_volume" "landing" {
  catalog_name = databricks_catalog.this.name
  schema_name  = databricks_schema.this["raw"].name
  name         = "landing"
  volume_type  = "MANAGED"
  comment      = "Source files: home_credit/<table>/, ecb/, streaming micro-files and checkpoints"
}

# Empty by default: on a single-user workspace the owner already has every privilege.
resource "databricks_grant" "catalog" {
  for_each = var.catalog_grants

  catalog    = databricks_catalog.this.name
  principal  = each.key
  privileges = each.value
}
