# Terraform owns catalogs, schemas, volumes and grants; bundles own pipelines and jobs.

locals {
  schemas = {
    raw     = "Landing zone: volumes with source files as delivered"
    bronze  = "Raw ingested tables, source schema plus ingestion metadata"
    silver  = "Cleaned, typed and deduplicated tables"
    gold    = "Business-level marts (client_month)"
    history = "SCD2 history base and model-validation snapshots"
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
