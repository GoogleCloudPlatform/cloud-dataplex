# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

locals {
  datasets = flatten([
    for dataset in var.bq_datasets_tables : [
      for table in dataset.tables : {
       dataset_id = dataset.dataset_id
       tables = table
      }
    ]
  ])
}

resource "google_dataplex_datascan" "data-profile-scan" {
  for_each = {
    for table in local.datasets:
      "${table.dataset_id}_${table.tables}" => table
  }

  data_scan_id = "${replace(each.value.dataset_id,"_", "-")}-${replace(each.value.tables,"_", "-")}-profile"
  description  = null
  display_name = "${replace(each.value.dataset_id,"_", "-")}-${replace(each.value.tables,"_", "-")}-profile"
  labels       = null
  location     = var.region
  project      = var.project_id
  data {
    entity   = null
    resource = "//bigquery.googleapis.com/projects/${var.project_id}/datasets/${each.value.dataset_id}/tables/${each.value.tables}"
  }
  data_profile_spec {
    row_filter       = null
    sampling_percent = 0
    post_scan_actions {}
  }
  execution_spec {
    field = null
    trigger {
      on_demand {
      }
    }
  }
  timeouts {
    create = null
    delete = null
    update = null
  }
}