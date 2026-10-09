variable "catalog" {
  description = "Unity Catalog catalog of the environment (credit_risk_dev / credit_risk_prod)."
  type        = string
}

variable "databricks_profile" {
  description = "Profile in ~/.databrickscfg used for authentication."
  type        = string
  default     = "DEFAULT"
}
