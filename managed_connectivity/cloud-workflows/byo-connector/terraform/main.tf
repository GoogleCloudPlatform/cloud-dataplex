module "cloud_workflows" {
  source  = "GoogleCloudPlatform/cloud-workflows/google"
  version = "0.1.1"
  workflow_name             = var.workflow_name
  project_id                = var.project_id
  region                    = var.region
  service_account_email     = var.service_account
  workflow_trigger = {
    cloud_scheduler = {
      name                  = "${var.workflow_name}-scheduler"
      cron                  = "0 0 * * *"
      time_zone             = "UTC"
      service_account_email = var.service_account
      deadline              = "1800s"
      argument              = jsonencode(var.workflow_args)
    }
  }
  workflow_source = var.workflow_source
}