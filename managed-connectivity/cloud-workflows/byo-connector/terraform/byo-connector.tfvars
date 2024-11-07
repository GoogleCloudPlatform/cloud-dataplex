project_id                      = "PROJECT_ID"
region                          = "LOCATION_ID"
service_account                 = "SERVICE_ACCOUNT_ID"
cron_schedule                   = "CRON_SCHEDULE_EXPRESSION"
workflow_args                   = {"TARGET_PROJECT_ID": "PROJECT_ID", "CLOUD_REGION": "LOCATION_ID", "TARGET_ENTRY_GROUP_ID": "ENTRY_GROUP_ID", "CREATE_TARGET_ENTRY_GROUP": CREATE_ENTRY_GROUP_BOOLEAN, "CLOUD_STORAGE_BUCKET_ID": "BUCKET_ID", "SERVICE_ACCOUNT": "SERVICE_ACCOUNT_ID", "ADDITIONAL_CONNECTOR_ARGS": [ADDITIONAL_CONNECTOR_ARGUMENTS], "CUSTOM_CONTAINER_IMAGE": "CONTAINER_IMAGE", "IMPORT_JOB_SCOPE_ENTRY_TYPES": [ENTRY_TYPES], "IMPORT_JOB_SCOPE_ASPECT_TYPES": [ASPECT_TYPES], "IMPORT_JOB_LOG_LEVEL": "INFO", "NETWORK_TAGS": [], "NETWORK_URI": "", "SUBNETWORK_URI": ""}


workflow_source                 = <<EOF
main:
  params: [args]
  steps:
    - init:
        assign:
        - WORKFLOW_ID: $${"metadataworkflow-" + sys.get_env("GOOGLE_CLOUD_WORKFLOW_EXECUTION_ID")}
        - NETWORK_URI: $${default(map.get(args, "NETWORK_URI"), "")}
        - SUBNETWORK_URI: $${default(map.get(args, "SUBNETWORK_URI"), "")}
        - NETWORK_TAGS: $${default(map.get(args, "NETWORK_TAGS"), [])}

    - check_networking:
        switch:
          - condition: $${NETWORK_URI != "" and SUBNETWORK_URI != ""}
            raise: "Error: cannot set both network_uri and subnetwork_uri. Please select one."
          - condition: $${NETWORK_URI != ""}
            steps:
              - submit_extract_job_with_network_uri:
                  assign:
                    - NETWORKING: $${NETWORK_URI}
                    - NETWORK_TYPE: "networkUri"
          - condition: $${SUBNETWORK_URI != ""}
            steps:
              - submit_extract_job_with_subnetwork_uri:
                  assign:
                    - NETWORKING: $${SUBNETWORK_URI}
                    - NETWORK_TYPE: "subnetworkUri"
        next: set_default_networking

    - set_default_networking:
        assign:
          - NETWORK_TYPE: "networkUri"
          - NETWORKING: $${"projects/" + args.TARGET_PROJECT_ID + "/global/networks/default"}
        next: check_create_target_entry_group

    - check_create_target_entry_group:
        switch:
          - condition: $${args.CREATE_TARGET_ENTRY_GROUP == true}
            next: create_target_entry_group
          - condition: $${args.CREATE_TARGET_ENTRY_GROUP == false}
            next: generate_extract_job_link

    - create_target_entry_group:
        call: http.post
        args:
          url: $${"https://dataplex.googleapis.com/v1/projects/" + args.TARGET_PROJECT_ID + "/locations/" + args.CLOUD_REGION + "/entryGroups?entry_group_id=" + args.TARGET_ENTRY_GROUP_ID}
          auth:
            type: OAuth2
            scopes: "https://www.googleapis.com/auth/cloud-platform"
        next: generate_extract_job_link

    - generate_extract_job_link:
        call: sys.log
        args:
            data: $${"https://console.cloud.google.com/dataproc/batches/" + args.CLOUD_REGION + "/" + WORKFLOW_ID + "/monitoring?project=" + args.TARGET_PROJECT_ID}
            severity: "INFO"
        next: submit_pyspark_extract_job

    - submit_pyspark_extract_job:
        call: http.post
        args:
          url: $${"https://dataproc.googleapis.com/v1/projects/" + args.TARGET_PROJECT_ID + "/locations/" + args.CLOUD_REGION + "/batches"}
          auth:
            type: OAuth2
            scopes: "https://www.googleapis.com/auth/cloud-platform"
          headers:
            Content-Type: "application/json"
          query:
            batchId: $${WORKFLOW_ID}
          body:
            pysparkBatch:
              mainPythonFileUri: file:///main.py
              args:
                - $${"--target_project_id=" + args.TARGET_PROJECT_ID}
                - $${"--target_location_id=" + args.CLOUD_REGION}
                - $${"--target_entry_group_id=" + args.TARGET_ENTRY_GROUP_ID}
                - $${"--output_bucket=" + args.CLOUD_STORAGE_BUCKET_ID}
                - $${"--output_folder=" + WORKFLOW_ID}
                - $${args.ADDITIONAL_CONNECTOR_ARGS}
            runtimeConfig:
                containerImage: $${args.CUSTOM_CONTAINER_IMAGE}
            environmentConfig:
                executionConfig:
                    serviceAccount: $${args.SERVICE_ACCOUNT}
                    stagingBucket: $${args.CLOUD_STORAGE_BUCKET_ID}
                    $${NETWORK_TYPE}: $${NETWORKING}
                    networkTags: $${NETWORK_TAGS}
        result: RESPONSE_MESSAGE
        next: check_pyspark_extract_job

    - check_pyspark_extract_job:
        call: http.get
        args:
          url: $${"https://dataproc.googleapis.com/v1/projects/" + args.TARGET_PROJECT_ID + "/locations/" + args.CLOUD_REGION + "/batches/" + WORKFLOW_ID}
          auth:
            type: OAuth2
            scopes: "https://www.googleapis.com/auth/cloud-platform"
        result: PYSPARK_EXTRACT_JOB_STATUS
        next: check_pyspark_extract_job_done

    - check_pyspark_extract_job_done:
        switch:
          - condition: $${PYSPARK_EXTRACT_JOB_STATUS.body.state == "SUCCEEDED"}
            next: generate_import_logs_link
          - condition: $${PYSPARK_EXTRACT_JOB_STATUS.body.state == "CANCELLED"}
            raise: $${PYSPARK_EXTRACT_JOB_STATUS}
          - condition: $${PYSPARK_EXTRACT_JOB_STATUS.body.state == "FAILED"}
            raise: $${PYSPARK_EXTRACT_JOB_STATUS}
        next: pyspark_extract_job_wait

    - pyspark_extract_job_wait:
        call: sys.sleep
        args:
          seconds: 30
        next: check_pyspark_extract_job
  
    - generate_import_logs_link:
        call: sys.log
        args:
            data: $${"https://console.cloud.google.com/logs/query?project=" + args.TARGET_PROJECT_ID + "&query=resource.type%3D%22dataplex.googleapis.com%2FMetadataJob%22+AND+resource.labels.location%3D%22" + args.CLOUD_REGION + "%22+AND+resource.labels.metadata_job_id%3D%22" + WORKFLOW_ID + "%22"}
            severity: "INFO"
        next: submit_import_job

    - submit_import_job:
        call: http.post
        args:
          url: $${"https://dataplex.googleapis.com/v1/projects/" + args.TARGET_PROJECT_ID + "/locations/" + args.CLOUD_REGION + "/metadataJobs?metadata_job_id=" + WORKFLOW_ID}
          auth:
            type: OAuth2
            scopes: "https://www.googleapis.com/auth/cloud-platform"
          body:
            type: IMPORT
            import_spec:
              source_storage_uri: $${"gs://" + args.CLOUD_STORAGE_BUCKET_ID + "/" + WORKFLOW_ID + "/"}
              entry_sync_mode: FULL
              aspect_sync_mode: INCREMENTAL
              log_level: $${default(map.get(args, "IMPORT_JOB_LOG_LEVEL"), "INFO")}
              scope:
                entry_groups: 
                  - $${"projects/" + args.TARGET_PROJECT_ID + "/locations/" + args.CLOUD_REGION + "/entryGroups/" + args.TARGET_ENTRY_GROUP_ID}
                entry_types: $${args.IMPORT_JOB_SCOPE_ENTRY_TYPES}
                aspect_types: $${args.IMPORT_JOB_SCOPE_ASPECT_TYPES}
        result: IMPORT_JOB_RESPONSE
        next: get_job_start_time

    - get_job_start_time:
        assign:
          - importJobStartTime: $${sys.now()}
        next: import_job_startup_wait

    - import_job_startup_wait:
        call: sys.sleep
        args:
          seconds: 30
        next: initial_get_import_job

    - initial_get_import_job:
        call: http.get
        args:
          url: $${"https://dataplex.googleapis.com/v1/projects/" + args.TARGET_PROJECT_ID + "/locations/" + args.CLOUD_REGION + "/metadataJobs/" + WORKFLOW_ID}
          auth:
            type: OAuth2
            scopes: "https://www.googleapis.com/auth/cloud-platform"
        result: IMPORT_JOB_STATUS
        next: check_import_job_status_available
  
    - check_import_job_status_available:
        switch:
          - condition: $${sys.now() - importJobStartTime > 300}  # 5 minutes = 300 seconds
            next: kill_import_job
          - condition: $${"status" in IMPORT_JOB_STATUS.body}
            next: check_import_job_done
        next: import_job_status_wait
  
    - import_job_status_wait:
        call: sys.sleep
        args:
          seconds: 30
        next: check_import_job_status_available

    - check_import_job_done:
        switch:
          - condition: $${IMPORT_JOB_STATUS.body.status.state == "SUCCEEDED"}
            next: the_end
          - condition: $${IMPORT_JOB_STATUS.body.status.state == "CANCELLED"}
            raise: $${IMPORT_JOB_STATUS}
          - condition: $${IMPORT_JOB_STATUS.body.status.state == "SUCCEEDED_WITH_ERRORS"}
            raise: $${IMPORT_JOB_STATUS}
          - condition: $${IMPORT_JOB_STATUS.body.status.state == "FAILED"}
            raise: $${IMPORT_JOB_STATUS}
          - condition: $${sys.now() - importJobStartTime > 43200}  # 12 hours = 43200 seconds
            next: kill_import_job
        next: import_job_wait

    - get_import_job:
        call: http.get
        args:
          url: $${"https://dataplex.googleapis.com/v1/projects/" + args.TARGET_PROJECT_ID + "/locations/" + args.CLOUD_REGION + "/metadataJobs/" + WORKFLOW_ID}
          auth:
            type: OAuth2
            scopes: "https://www.googleapis.com/auth/cloud-platform"
        result: IMPORT_JOB_STATUS
        next: check_import_job_done

    - import_job_wait:
        call: sys.sleep
        args:
          seconds: 30
        next: get_import_job

    - kill_import_job:
        call: http.post
        args:
          url: $${"https://dataplex.googleapis.com/v1/projects/" + args.TARGET_PROJECT_ID + "/locations/" + args.CLOUD_REGION + "/metadataJobs/" + WORKFLOW_ID + ":cancel"}
          auth:
            type: OAuth2
            scopes: "https://www.googleapis.com/auth/cloud-platform"
        next: get_killed_import_job

    - get_killed_import_job:
        call: http.get
        args:
          url: $${"https://dataplex.googleapis.com/v1/projects/" + args.TARGET_PROJECT_ID + "/locations/" + args.CLOUD_REGION + "/metadataJobs/" + WORKFLOW_ID}
          auth:
            type: OAuth2
            scopes: "https://www.googleapis.com/auth/cloud-platform"
        result: KILLED_IMPORT_JOB_STATUS
        next: killed

    - killed:
        raise: $${KILLED_IMPORT_JOB_STATUS}

    - the_end:
        return: $${IMPORT_JOB_STATUS}
EOF
