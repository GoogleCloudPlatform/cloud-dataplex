import argparse
from google.cloud import dataplex_v1


parser = argparse.ArgumentParser()
parser.add_argument('--datascan_name', '-dsn', help='Full project path of the Datascan. Allowed format = "projects/{PROJECT-ID}/locations/{REGION}/dataScans/{DATASCAN-NAME}"')
args = parser.parse_args()


def run_data_scan():
    # Create a Dataplex client object
    print("Authenticating Dataplex Client...")
    dataplex_client = dataplex_v1.DataScanServiceClient()

    # Define a RunDataScanRequest()
    datascan_name = args.datascan_name # format: "projects/{PROJECT-ID}/locations/{REGION}/dataScans/{DATASCAN-NAME}"
    run_request = dataplex_v1.RunDataScanRequest(
        name = datascan_name
    )
    print("Starting run job for scan...")
    run_scan_job = dataplex_client.run_data_scan(request=run_request)

    # Job Execution Started:
    print("Job ID --> " + str(run_scan_job.job.uid))
    print("Status --> " + str(run_scan_job.job.state))
    print("Data Quality Spec --> " + str(run_scan_job.job.data_quality_spec))
    print("Job started successfully!!")


run_data_scan()