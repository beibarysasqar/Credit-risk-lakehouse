# Auth comes from a profile in ~/.databrickscfg; no host or token in code.
provider "databricks" {
  profile = var.databricks_profile
}
