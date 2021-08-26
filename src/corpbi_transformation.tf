resource "aws_s3_bucket_object" "Load_ActivityDaily_Contracts" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain.Load_ActivityDaily_Contracts.py"
  source = "scripts/CorpBI/domain.Load_ActivityDaily_Contracts.py"
  etag   = "${md5(file("scripts/CorpBI/domain.Load_ActivityDaily_Contracts.py"))}"
}

resource "aws_glue_job" "Load_ActivityDaily_Contracts" {
  name               = "domain.Load_ActivityDaily_Contracts"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.load_contractstatistics_dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_ActivityDaily_Contracts.id}"
  }

  default_arguments = {
    "--domain_db"                      = "${var.domain_database}"
    "--contracts_table"                = "${var.domain_contracts_table}"
    "--linkedaccountantlog_table"      = "${var.domain_linkedaccountantlog_table}"
    "--accountscontract_summary_table" = "${var.domain_accountscontractsummary_table}"
    "--accounts_table"                 = "${var.domain_accounts_table}"
    "--s3_destination"                 = "s3://${var.domain_bucket}/Data/ActivityDaily_Contracts"
    "--job-bookmark-option"            = "${var.bookmark}"
  }
}
resource "aws_s3_bucket_object" "Accounts" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain.Load_Accounts.py"
  source = "scripts/CorpBI/domain.Load_Accounts.py"
  etag   = "${md5(file("scripts/CorpBI/domain.Load_Accounts.py"))}"
}

resource "aws_glue_job" "Load_Accounts" {
  name               = "domain.Load_Accounts"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.load_contractstatistics_dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Accounts.id}"
  }

  default_arguments = {
    "--extra-py-files"                     = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                              = "${var.raw_database}"
    "--raw_contractstatistics_corpbi_table" = "${var.raw_contractstatistics_table}"
    "--raw_accounts_table"      = "${var.raw_accounts_table}"
    "--s3_destination"                      = "s3://${var.domain_bucket}/Data/Accounts"
    "--job-bookmark-option"                 = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Load_CorpBI_Transformations" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain.Load_CorpBI_Transformations.py"
  source = "scripts/CorpBI/domain.Load_CorpBI_Transformations.py"
  etag   = "${md5(file("scripts/CorpBI/domain.Load_CorpBI_Transformations.py"))}"
}

resource "aws_glue_job" "Load_CorpBI_Transformations" {
  name               = "domain.Load_CorpBI_Transformations"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.load_contractstatistics_dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_CorpBI_Transformations.id}"
  }

  default_arguments = {
    "--extra-py-files"                     = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--raw_db"                             = "${var.raw_database}"
    "--domain_db"                          = "${var.domain_database}"
    "--raw_monthlyappusage_table"          = "${var.raw_monthlyappusage_table}"
    "--raw_contractstatistics_table"       = "${var.raw_contractstatistics_table}"
    "--raw_appbillingproposal_table"       = "${var.raw_appbillingproposal_table}"
    "--raw_opportunities_table"            = "${var.raw_opportunities_table}"
    "--domain_users_table"                 = "${var.domain_users_table}"
    "--contracteventtypes_s3_destination"  = "s3://${var.domain_bucket}/Data/ContractEventTypes"
    "--itemclassifications_s3_destination" = "s3://${var.domain_bucket}/Data/ItemClassifications"
    "--resellers_s3_destination"           = "s3://${var.domain_bucket}/Data/Resellers"
    "--subsectors_s3_destination"          = "s3://${var.domain_bucket}/Data/SubSectors"
    "--sectors_s3_destination"             = "s3://${var.domain_bucket}/Data/Sectors"
    "--accountants_s3_destination"         = "s3://${var.domain_bucket}/Data/Accountants"
    "--appbillingproposal_s3_destination"  = "s3://${var.domain_bucket}/Data/AppBillingProposal"
    "--opportunities_s3_destination"       = "s3://${var.domain_bucket}/Data/Opportunities"
    "--monthlyappusage_s3_destination"     = "s3://${var.domain_bucket}/Data/MonthlyAppUsage"
    "--contracts_s3_destination"           = "s3://${var.domain_bucket}/Data/Contracts"
    "--accountmanager_s3_destination"      = "s3://${var.domain_bucket}/Data/AccountManager"
    "--job-bookmark-option"                = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Load_NoActivity_Dashboard" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain.Load_NoActivity_Dashboard.py"
  source = "scripts/CorpBI/domain.Load_NoActivity_Dashboard.py"
  etag   = "${md5(file("scripts/CorpBI/domain.Load_NoActivity_Dashboard.py"))}"
}

resource "aws_glue_job" "Load_NoActivity_Dashboard" {
  name               = "domain.Load_NoActivity_Dashboard"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.load_contractstatistics_dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Load_NoActivity_Dashboard.id}"
  }

  default_arguments = {
    "--domain_db"                             = "${var.domain_database}"
    "--config_calendar_table"                 = "${var.config_calendar_table}"
    "--config_activities_table"               = "${var.config_activities_table}"
    "--domain_activitydaily_table"            = "${var.domain_activitydaily_table}"
    "--domain_accountscontract_summary_table" = "${var.domain_accountscontractsummary_table}"
    "--domain_accounts_table"                 = "${var.domain_accounts_table}"
    "--domain_users_table"                    = "${var.domain_users_table}"
    "--s3_destination"                        = "s3://${var.domain_bucket}/Data/NoActivity_Dashboard"
  }
}

resource "aws_s3_bucket_object" "AccountSummary_Monthly" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain.Load_AccountSummary_Monthly.py"
  source = "scripts/CorpBI/domain.Load_AccountSummary_Monthly.py"
  etag   = "${md5(file("scripts/CorpBI/domain.Load_AccountSummary_Monthly.py"))}"
}

resource "aws_glue_job" "Load_AccountSummary_Monthly" {
  name               = "domain.Load_AccountSummary_Monthly"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.load_contractstatistics_dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.AccountSummary_Monthly.id}"
  }

  default_arguments = {
    "--start_month_index"   = "0"
    "--end_month_index"     = "-6"
    "--s3_destination"      = "s3://${var.domain_bucket}/Data/AccountsContract_PerMonth"
    "--job-bookmark-option" = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "LinkedAccountantLog_Transformation_Logic" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain_Load_LinkedAccountantLog_Transformation.py"
  source = "scripts/CorpBI/domain_Load_LinkedAccountantLog_Transformation.py"
  etag   = "${md5(file("scripts/CorpBI/domain_Load_LinkedAccountantLog_Transformation.py"))}"
}

resource "aws_s3_bucket_object" "LinkedAccountantLog" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain.Load_LinkedAccountantLog.py"
  source = "scripts/CorpBI/domain.Load_LinkedAccountantLog.py"
  etag   = "${md5(file("scripts/CorpBI/domain.Load_LinkedAccountantLog.py"))}"
}

resource "aws_glue_job" "Load_LinkedAccountantLog" {
  name               = "domain.Load_LinkedAccountantLog"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.load_contractstatistics_dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.LinkedAccountantLog.id}"
  }

  default_arguments = {
    "--extra-py-files"                     = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id},s3://${var.glue_artifact_store}/${aws_s3_bucket_object.LinkedAccountantLog_Transformation_Logic.id}"
    "--raw_db"                             = "${var.raw_database}"
    "--domain_db"                          = "${var.domain_database}"
    "--raw_contractstatistics_table"       = "${var.raw_contractstatistics_table}"
    "--domain_accounts_table"              = "${var.domain_accounts_table}"
    "--linkedaccountantlog_s3_destination" = "s3://${var.domain_bucket}/Data/LinkedAccountantLog"
    "--job-bookmark-option"                = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "AccountContractSummary" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain.Load_AccountsContract_Summary.py"
  source = "scripts/CorpBI/domain.Load_AccountsContract_Summary.py"
  etag   = "${md5(file("scripts/CorpBI/domain.Load_AccountsContract_Summary.py"))}"
}

resource "aws_glue_job" "Load_AccountsContract_Summary" {
  name               = "domain.Load_AccountsContract_Summary"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.load_contractstatistics_dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.AccountContractSummary.id}"
  }

  default_arguments = {
    "--extra-py-files"                   = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id},s3://${var.glue_artifact_store}/${aws_s3_bucket_object.AccountContractSummary_Transformation_Logic.id}"
    "--domain_db"                        = "${var.domain_database}"
    "--accounts_domain_table"            = "${var.domain_accounts_table}"
    "--contracts_domain_table"           = "${var.domain_contracts_table}"
    "--linkedaccountantlog_domain_table" = "${var.domain_linkedaccountantlog_table}"
    "--s3_destination"                   = "s3://${var.domain_bucket}/Data/AccountsContract_Summary"
    "--job-bookmark-option"              = "${var.bookmark}"
  }
}

resource "aws_s3_bucket_object" "Utils" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/utils.py"
  source = "scripts/CorpBI/utils.py"
  etag   = "${md5(file("scripts/CorpBI/utils.py"))}"
}

resource "aws_s3_bucket_object" "AccountContractSummary_Transformation_Logic" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain_Load_AccountsContract_Summary_Transformation.py"
  source = "scripts/CorpBI/domain_Load_AccountsContract_Summary_Transformation.py"
  etag   = "${md5(file("scripts/CorpBI/domain_Load_AccountsContract_Summary_Transformation.py"))}"
}

resource "aws_s3_bucket_object" "CustomerProfile_ContractHistory" {
  bucket = "${var.glue_artifact_store}"
  key    = "Glue/${var.environment}/CorpBI/domain.Load_CustomerProfile_ContractHistory.py"
  source = "scripts/CorpBI/domain.Load_CustomerProfile_ContractHistory.py"
  etag   = "${md5(file("scripts/CorpBI/domain.Load_CustomerProfile_ContractHistory.py"))}"
}

resource "aws_glue_job" "Load_CustomerProfile_ContractHistory" {
  name               = "domain.Load_CustomerProfile_ContractHistory"
  role_arn           = "${data.aws_iam_role.cig_glue_role.arn}"
  allocated_capacity = "${var.load_contractstatistics_dpu}"

  command {
    script_location = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.CustomerProfile_ContractHistory.id}"
  }
  default_arguments = {
    "--extra-py-files"                                 = "s3://${var.glue_artifact_store}/${aws_s3_bucket_object.Utils.id}"
    "--domain_db"                                      = "${var.domain_database}"
    "--contracts_domain_table"                         = "${var.domain_contracts_table}"
    "--itemclassifications_domain_table"               = "${var.domain_itemclassifications_table}"
    "--accountscontractsummary_domain_table"           = "${var.domain_accountscontractsummary_table}"
    "--contracteventtypes_domain_table"                = "${var.domain_contracteventtypes_table}"
    "--customerprofile_contracthistory_s3_destination" = "s3://${var.domain_bucket}/Data/CustomerProfileContractHistory"
    }
  }