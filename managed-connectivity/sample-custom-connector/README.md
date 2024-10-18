# Develop a custom connector for metadata import

This code sample provides a reference for you to build a custom connector that extracts metadata from a third-party source. You use the connector when running a [managed connectivity pipeline](https://cloud.google.com/dataplex/docs/managed-connectivity-overview) that imports metadata into Dataplex.

You can build connectors to extract metadata from third-party sources. For example, you can build a connector to extract data from sources like MySQL, SQL Server, Oracle, Snowflake, Databricks, and others.

Use the example connector in this document as a starting point to build your own connectors. The example connector connects to an Oracle Database Express Edition (XE) database. The connector is built in Python, though you can also use Java, Scala, or R.

See detailed documentation for the code and how to orchestrate managed connectivity pipeline to extract metadata using this sample connector in [Develop a custom connector for metadata import](https://cloud.google.com/dataplex/docs/develop-custom-connector). 

Create container image for sample connector and push to Artifact Registry:

`bash build_and_push_docker.sh`

To test metadata extraction locally without building container image:

1. Install PySpark for your project:

`pip install pyspark`

2. Download **ojdbc11.jar** (or the version you prefer).
3. Change driver path in [source code](src/oracle_connector.py).
4. Install dependencies from the requirements.txt:

`pip install -r requirements.txt`