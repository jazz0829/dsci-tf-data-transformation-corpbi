variable "region" {
  default = "eu-west-1"
}

variable "environment" {}

variable "glue_artifact_store" {
  default = "cig-build-artifact-store"
}

variable "bookmark" {
  default = "job-bookmark-disable"
}

variable "load_contractstatistics_dpu" {
  default = 10
}

variable "domain_bucket" {}

variable "raw_database" {
  default = "customerintelligence_raw"
}

variable "domain_database" {
  default = "customerintelligence"
}

variable "raw_contractstatistics_table" {
  default = "object_dw_contractstatistics"
}

variable "raw_accounts_eol_hosting_table" {
  default = "object_host_cig_accounts"
}

variable "raw_appbillingproposal_table" {
  default = "object_dw_appbillingproposal"
}

variable "raw_monthlyappusage_table" {
  default = "object_dw_monthlyappusage"
}

variable "raw_opportunities_table" {
  default = "object_dw_opportunities"
}

variable "domain_linkedaccountantlog_table" {
  default = "linkedaccountantlog"
}

variable "domain_accounts_table" {
  default = "accounts"
}

variable "domain_contracts_table" {
  default = "contracts"
}

variable "domain_users_table" {
  default = "users"
}

variable "domain_itemclassifications_table" {
  default = "itemclassifications"
}

variable "domain_accountscontractsummary_table" {
  default = "accountscontract_summary"
}

variable "domain_activitydaily_table" {
  default = "activitydaily"
}

variable "domain_contracteventtypes_table" {
  default = "contracteventtypes"
}

variable "config_calendar_table" {
  default = "config_calendar"
}

variable "config_activities_table" {
  default = "config_activities"
}

variable "raw_accounts_table" {
  default = "accounts"
}