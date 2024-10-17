# Dataplex Metadata Job Import API

To experiment with this pipeline, you don't have to run in on a cluster. It can be debugged locally.

To run locally:
1. Install PySpark for that project

`pip install pyspark`

2. Download **ojdbc11.jar** (or the version you prefer)
3. Change driver path in src/oracle_connector.py 
4. Install dependencies from the requirements.txt

`pip install -r requirements.txt`
