provider "aws" {
  region = "${var.region}"
}

resource "aws_glue_trigger" "corpbi_trigger" {
  name     = "trigger_0715_utc"
  type     = "SCHEDULED"
  schedule = "cron(15 7 ? * * *)"
  enabled  = "${var.environment == "prod" ? true : false }"

  actions {
    job_name = "${aws_glue_job.Load_Accounts.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_CorpBI_Transformations.name}"
  }
}

resource "aws_glue_trigger" "corpbi-conditional_trigger" {
  name    = "corpbi_conditional"
  type    = "CONDITIONAL"
  enabled = "${var.environment == "prod" ? true : false }"

  predicate {
    conditions {
      job_name         = "${aws_glue_job.Load_CorpBI_Transformations.name}"
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }

    conditions {
      job_name         = "${aws_glue_job.Load_Accounts.name}"
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }

    logical = "AND"
  }

  actions {
    job_name = "${aws_glue_job.Load_AccountSummary_Monthly.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_AccountsContract_Summary.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_LinkedAccountantLog.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_NoActivity_Dashboard.name}"
  }
}

resource "aws_glue_trigger" "corpbi-conditional_trigger_part_II" {
  name    = "corpbi_conditional_part_II"
  type    = "CONDITIONAL"
  enabled = "${var.environment == "prod" ? true : false }"

  predicate {
    conditions {
      job_name         = "${aws_glue_job.Load_AccountsContract_Summary.name}"
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }

    conditions {
      job_name         = "${aws_glue_job.Load_LinkedAccountantLog.name}"
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }

    logical = "AND"
  }

  actions {
    job_name = "${aws_glue_job.Load_ActivityDaily_Contracts.name}"
  }

  actions {
    job_name = "${aws_glue_job.Load_CustomerProfile_ContractHistory.name}"
  }
}
