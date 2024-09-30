variable "project_id" {
    default = ""
}

variable "region" {
    default = ""
}

variable "service_account" {
    default = ""
}

variable "workflow_name" {
    default = "managed-orchestration-for-dataplex"
}

variable "description" {
    default = "Submits a Dataproc Serverless Job and then runs a Dataplex Import Job. Times out after 12 hours."
}

variable "workflow_args" {
    default = {}
}

variable "cron_schedule" {
    default = "0 0 * * *"
}

variable "workflow_source" {
    default = ""
}