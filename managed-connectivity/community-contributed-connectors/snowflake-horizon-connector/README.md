Snowflake to Dataplex Metadata Loader

Foreword
In today's complex data landscape, organizations increasingly recognize data as a critical asset. The ability to effectively discover, understand, and govern this data is paramount for informed decision-making, regulatory compliance, and innovation. As data ecosystems grow, spanning various platforms and technologies, maintaining a holistic view of data assets becomes challenging.
This connector addresses a key need for many enterprises: bridging the gap between their data warehousing capabilities and the comprehensive data governance and discovery features offered by Google Cloud's Dataplex. Dataplex provides a unified data fabric to manage, monitor, and govern data across diverse environments within Google Cloud.
The Snowflake to Dataplex Data Catalog Connector, detailed in this guide, is a testament to the power of seamless integration. It's designed to automate metadata synchronization, bringing the rich context of your data into sataplex. This not only enhances data visibility and accessibility for all stakeholders but also strengthens your data governance by centralizing metadata management, lineage tracking, and data quality initiatives.
We believe that by leveraging this connector, you'll unlock new levels of efficiency and empower your teams to truly become data-driven. This guide is your indispensable companion, providing clear, step-by-step instructions and practical insights for a successful implementation.

Step by Step Guide
This document will show an end to end POC of how to use Snowflake to Dataplex - Data Catalog Loader

Step 1: Setting up Snowflake Environment from where you have to load the metadata. To access the Horizon catalog in Snowflake, you will need to use the ACCOUNT_USAGE views located under the SNOWFLAKE database.

Step 2: Storing the connecting details in Secret Manager.

So, we will got to GCP -> Secret Manager -> Create Secret ->
And will create the following 3 Secrets one by one:-
snowflake-user-name
snowflake-user-password
Snowflake-account-uri

Step 3: Setting up the Dataplex Universal Catalog Environment.

You will perform the following one-time setup steps:
Create an entry group for the entries that you want to import.

Create aspect types for the aspects that you want to import.
Create entry types for the entries that you want to import.

Go to Dataplex -> Dataplex Universal Catalog -> Catalog

Step 4: Execute the Python Script to load the Horizon Data Catalog from Snowflake to Dataplex
Now execute the Python script:- snowflake_to_dataplex_metadata_loader.py

This process involves several steps: first, installing necessary libraries; then, retrieving Snowflake connection details from the secret manager. Following this, a connection to Snowflake is established to extract Horizon catalog data, which is subsequently loaded directly into Dataplex.

Step 5: Validate everything in Dataplex.

