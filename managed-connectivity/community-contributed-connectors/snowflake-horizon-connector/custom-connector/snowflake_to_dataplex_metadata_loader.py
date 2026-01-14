# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Install necessary libraries if not already present in the environment
!pip install --user google-cloud-bigquery google-cloud-storage google-cloud-dataplex google-api-core functions-framework

!pip install --force-reinstall google-cloud-dataplex

!pip install snowflake-connector-python pandas

pip install snowflake-connector-python google-cloud-secret-manager

PROJECT_ID = "Enter your Project Name"

from google.cloud import secretmanager

import snowflake.connector
import pandas as pd

import os
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import dataplex_v1
import datetime
from google.protobuf import struct_pb2

from google.cloud.dataplex_v1.types import Schema
from google.protobuf import json_format
import json

def access_secret_version(secret_id, PROJECT_ID, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

SNOWFLAKE_USER_NAME = access_secret_version(
        secret_id="snowflake-user-name",
        PROJECT_ID=f"{PROJECT_ID}"
    )
SNOWFLAKE_PASSWORD = access_secret_version(
        secret_id="snowflake-user-password",
        PROJECT_ID=f"{PROJECT_ID}"
    )
SNOWFLAKE_ACCOUNT_URI = access_secret_version(
        secret_id="snowflake-account-uri",
        PROJECT_ID=f"{PROJECT_ID}"
    )

print(SNOWFLAKE_USER_NAME)
print(SNOWFLAKE_PASSWORD)
print(SNOWFLAKE_ACCOUNT_URI)

# fetch the details from Secret Manager
SNOWFLAKE_ACCOUNT = SNOWFLAKE_ACCOUNT_URI
SNOWFLAKE_USER = SNOWFLAKE_USER_NAME
SNOWFLAKE_PASSWORD = SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE = 'Enter your Warehouse'
SNOWFLAKE_DATABASE = 'Enter your Database'
SNOWFLAKE_SCHEMA = 'Enter your Schema'

try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    print("Successfully connected to Snowflake!")
except Exception as e:
    print(f"Error connecting to Snowflake: {e}")

# Query 1: DATABASE
try:
    cursor_db = conn.cursor()
    query_db = "select * from snowflake.account_usage.databases;"
    cursor_db.execute(query_db)

    # Fetch all results and load into a Pandas DataFrame
    df_db = pd.DataFrame(cursor_db.fetchall(), columns=[col[0] for col in cursor_db.description])
    print("\n--- Output for ACCOUNT_USUAGE.DATABASES ---")
    print(df_db.head()) # Print first few rows
    print(f"Total rows for DATABASE: {len(df_db)}")

except Exception as e:
    print(f"Error executing DATABASE query: {e}")
finally:
    if 'cursor_db' in locals() and cursor_db:
        cursor_db.close()

# --- Configuration Variables ---
PROJECT_ID = "Enter your project_id"
DATAPLEX_LOCATION = "Enter dataplex location"
DATAPLEX_LAKE_ID = "Enter snowflake lake_id"

print(f"Project ID: {PROJECT_ID}")
print(f"Dataplex Lake: {DATAPLEX_LAKE_ID} in {DATAPLEX_LOCATION}")

dataplex_client = dataplex_v1.DataplexServiceClient()

for _, row in df_db.iterrows():
    DATABASE_NAME = row['DATABASE_NAME']
    DATABASE_OWNER = row['DATABASE_OWNER']
    DATABASE_COMMENT = row['COMMENT']
    DATABASE_CREATED = str(row['CREATED'])
    DATABASE_LAST_ALTERED = str(row['LAST_ALTERED'])
    DATABASE_TYPE = row['TYPE']
    print(DATABASE_NAME,DATABASE_OWNER,DATABASE_COMMENT,DATABASE_CREATED,DATABASE_LAST_ALTERED,DATABASE_TYPE)
    print(type(DATABASE_CREATED))
    entry_id = f"{DATABASE_NAME}".replace('.', '-').replace(' ', '_').upper()
    entry_id = entry_id[:40].strip('-').strip('_')
    print(entry_id)

    entry_group_id = "snowflakehorizongrp" #enter your entry group id for snowflake
    entry_type_id = "snowhorizondb" #enter your entry type id for snowflake dbs
    name = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}/entries/{entry_id}"
    print(name)
    entry_type = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryTypes/{entry_type_id}"
    print(entry_type)
    aspects={
                f"{PROJECT_ID}.{DATAPLEX_LOCATION}.{entry_type_id}": dataplex_v1.Aspect(
                    aspect_type=f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/aspectTypes/{entry_type_id}",
                    data=struct_pb2.Struct(
                        fields={
                            "databasename": struct_pb2.Value(string_value=f"{DATABASE_NAME}"),
                            "databaseowner": struct_pb2.Value(string_value=f"{DATABASE_OWNER}"),
                            "databasecomment": struct_pb2.Value(string_value=f"{DATABASE_COMMENT}"),
                            "databasecreated": struct_pb2.Value(string_value=f"{DATABASE_CREATED}"),
                            "databaselastaltered": struct_pb2.Value(string_value=f"{DATABASE_LAST_ALTERED}"),
                            "databasetype": struct_pb2.Value(string_value=f"{DATABASE_TYPE}"),
                        }
                    ),
                )
            }
    print(aspects)

    entry = dataplex_v1.Entry()
    entry.name = name
    entry.entry_type = entry_type
    entry.aspects = aspects
    parent = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}"
    print(parent)
    client = dataplex_v1.CatalogServiceClient()
    response = client.create_entry(parent=parent, entry=entry, entry_id=entry_id)
    print("Database successfully loaded in Dataplex")

# Query 2: SCHEMA
try:
    cursor_schema = conn.cursor()
    query_schema = "select * from snowflake.account_usage.schemata;"
    cursor_schema.execute(query_schema)

    # Fetch all results and load into a Pandas DataFrame
    df_schema = pd.DataFrame(cursor_schema.fetchall(), columns=[col[0] for col in cursor_schema.description])
    print("\n--- Output for ACCOUNT_USUAGE.SCHEMA ---")
    print(df_schema.head()) # Print first few rows
    print(f"Total rows for SCHEMA: {len(df_schema)}")

except Exception as e:
    print(f"Error executing SCHEMA query: {e}")
finally:
    if 'cursor_schema' in locals() and cursor_schema:
        cursor_schema.close()

for _, row in df_schema.iterrows():
    SCHEMA_NAME = row['SCHEMA_NAME']
    SCHEMA_OWNER = row['SCHEMA_OWNER']
    SCHEMA_COMMENT = row['COMMENT']
    SCHEMA_CREATED = str(row['CREATED'])
    SCHEMA_LAST_ALTERED = str(row['LAST_ALTERED'])
    SCHEMA_TYPE = row['SCHEMA_TYPE']
    SCHEMA_CATALOG_NAME = row['CATALOG_NAME']
    print(SCHEMA_NAME,SCHEMA_OWNER,SCHEMA_COMMENT,SCHEMA_CREATED,SCHEMA_LAST_ALTERED,SCHEMA_TYPE,SCHEMA_CATALOG_NAME)
    print(type(SCHEMA_CREATED))
    entry_id = f"{SCHEMA_NAME}".replace('.', '-').replace(' ', '_').upper()
    entry_id = entry_id[:40].strip('-').strip('_')
    print(entry_id)

    entry_group_id = "snowflakehorizongrp" #enter your entry group id for snowflake
    entry_type_id = "snowhorizonschema" #enter your entry type id for snowflake schemas
    name = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}/entries/{entry_id}"
    print(name)
    entry_type = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryTypes/{entry_type_id}"
    print(entry_type)
    aspects={
                f"{PROJECT_ID}.{DATAPLEX_LOCATION}.{entry_type_id}": dataplex_v1.Aspect(
                    aspect_type=f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/aspectTypes/{entry_type_id}",
                    data=struct_pb2.Struct(
                        fields={
                            "schemaname": struct_pb2.Value(string_value=f"{SCHEMA_NAME}"),
                            "schemaowner": struct_pb2.Value(string_value=f"{SCHEMA_OWNER}"),
                            "schemacomment": struct_pb2.Value(string_value=f"{SCHEMA_COMMENT}"),
                            "schemacreated": struct_pb2.Value(string_value=f"{SCHEMA_CREATED}"),
                            "schemalastaltered": struct_pb2.Value(string_value=f"{SCHEMA_LAST_ALTERED}"),
                            "schematype": struct_pb2.Value(string_value=f"{SCHEMA_TYPE}"),
                            "schemacatalogname": struct_pb2.Value(string_value=f"{SCHEMA_CATALOG_NAME}"),
                        }
                    ),
                )
            }
    print(aspects)

    entry = dataplex_v1.Entry()
    entry.name = name
    entry.entry_type = entry_type
    entry.aspects = aspects
    parent = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}"
    print(parent)
    client = dataplex_v1.CatalogServiceClient()
    response = client.create_entry(parent=parent, entry=entry, entry_id=entry_id)
    print("Schema successfully loaded in Dataplex")

# Final Query 3: TABLES & COLUMNS
try:
    cursor_table = conn.cursor()
    query_table = "select distinct table_name from (select a.*,b.tag_name, b.tag_value from snowflake.account_usage.columns as a left join snowflake.account_usage.tag_references as b on a.column_name = b.column_name);"
    cursor_table.execute(query_table)

    # Fetch all results and load into a Pandas DataFrame
    df_table = pd.DataFrame(cursor_table.fetchall(), columns=[col[0] for col in cursor_table.description])
    print("\n--- Output for ACCOUNT_USUAGE.TABLES ---")
    print(df_table.head()) # Print first few rows
    print(f"Total rows for TABLE: {len(df_table)}")

except Exception as e:
    print(f"Error executing TABLE query: {e}")
finally:
    if 'cursor_table' in locals() and cursor_table:
        cursor_table.close()

# Final Query 3: Table & COLUMNS
for _, row in df_table.iterrows():
  TABLE_NAME = row['TABLE_NAME']
  C_TABLE_NAME = f"'{TABLE_NAME}'"
  print(C_TABLE_NAME)
  try:
      cursor_columns = conn.cursor()
      query_columns = f"select a.*,b.tag_name, b.tag_value,CASE WHEN a.DATA_TYPE IN ('TEXT', 'STRING') THEN 'STRING' WHEN a.DATA_TYPE = 'DATE' THEN 'DATETIME' WHEN a.DATA_TYPE IN ('NUMBER', 'INTEGER') THEN 'NUMBER' ELSE a.DATA_TYPE END AS metadataType from snowflake.account_usage.columns as a left join snowflake.account_usage.tag_references as b on a.column_name = b.column_name where a.TABLE_NAME = {C_TABLE_NAME};"
      cursor_columns.execute(query_columns)

      # Fetch all results and load into a Pandas DataFrame
      df_columns = pd.DataFrame(cursor_columns.fetchall(), columns=[col[0] for col in cursor_columns.description])
      print("\n--- Output for ACCOUNT_USUAGE.COLUMNS ---")
      print(df_columns.head()) # Print first few rows
      print(f"Total rows for COLUMNS: {len(df_columns)}")

  except Exception as e:
      print(f"Error executing COLUMNS query: {e}")
  finally:
      if 'cursor_columns' in locals() and cursor_columns:
          cursor_columns.close()
  entry_id = TABLE_NAME
  print(entry_id)
  entry_group_id = "snowflakehorizongrp" #enter your entry group id for snowflake
  entry_type_id = "snowtesttable" #enter your entry type id for snowflake tables
  name = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}/entries/{entry_id}"
  print(name)
  entry_type = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryTypes/{entry_type_id}"
  print(entry_type)
  schema_json = {
    "fields": []
    }
  for index, row in df_columns.iterrows():
    field = {
        "name": row['COLUMN_NAME'],
        "dataType": row['DATA_TYPE'],
        "metadataType": row['METADATATYPE'],
        "mode": "NULLABLE"
    }
    schema_json["fields"].append(field)
  print(json.dumps(schema_json, indent=2))

  aspects={
                "dataplex-types.global.schema": dataplex_v1.Aspect(
                    aspect_type="projects/dataplex-types/locations/global/aspectTypes/schema",
                    data= schema_json,)
  }
  print(aspects)
  entry = dataplex_v1.Entry()
  entry.name = name
  entry.entry_type = entry_type
  entry.aspects = aspects
  parent = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}"
  print(parent)
  client = dataplex_v1.CatalogServiceClient()
  response = client.create_entry(parent=parent, entry=entry, entry_id=entry_id)
  print("column entry created successfully")

# Query 4: TAGS
try:
    cursor_tags = conn.cursor()
    query_tags = "select * from snowflake.account_usage.tags;"
    cursor_tags.execute(query_tags)

    # Fetch all results and load into a Pandas DataFrame
    df_tags = pd.DataFrame(cursor_tags.fetchall(), columns=[col[0] for col in cursor_tags.description])
    print("\n--- Output for ACCOUNT_USUAGE.TAGS ---")
    print(df_tags.head()) # Print first few rows
    print(f"Total rows for TAGS: {len(df_tags)}")

except Exception as e:
    print(f"Error executing TAGS query: {e}")
finally:
    if 'cursor_tags' in locals() and cursor_tags:
        cursor_tags.close()

for _, row in df_tags.iterrows():
    TAG_NAME = row['TAG_NAME']
    TAG_SCHEMA = row['TAG_SCHEMA']
    TAG_DATABASE = row['TAG_DATABASE']
    TAG_OWNER = row['TAG_OWNER']
    TAG_COMMENT = row['TAG_COMMENT']
    TAG_CREATED = str(row['CREATED'])
    TAG_LAST_ALTERED = str(row['LAST_ALTERED'])
    TAG_ALLOWED_VALUES = str(row['ALLOWED_VALUES'])
    print(TAG_NAME,TAG_SCHEMA,TAG_DATABASE,TAG_OWNER,TAG_COMMENT,TAG_CREATED,TAG_LAST_ALTERED,TAG_ALLOWED_VALUES)
    print(type(TAG_ALLOWED_VALUES))
    entry_id = f"{TAG_NAME}".replace('.', '-').replace(' ', '_').upper()
    entry_id = entry_id[:40].strip('-').strip('_')
    print(entry_id)

    entry_group_id = "snowflakehorizongrp" #enter your entry group id for snowflake
    entry_type_id = "snowhorizontag" #enter your entry type id for snowflake tags
    name = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}/entries/{entry_id}"
    print(name)
    entry_type = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryTypes/{entry_type_id}"
    print(entry_type)
    aspects={
                f"{PROJECT_ID}.{DATAPLEX_LOCATION}.{entry_type_id}": dataplex_v1.Aspect(
                    aspect_type=f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/aspectTypes/{entry_type_id}",
                    data=struct_pb2.Struct(
                        fields={
                            "tagname": struct_pb2.Value(string_value=f"{TAG_NAME}"),
                            "tagschema": struct_pb2.Value(string_value=f"{TAG_SCHEMA}"),
                            "tagdatabase": struct_pb2.Value(string_value=f"{TAG_DATABASE}"),
                            "tagowner": struct_pb2.Value(string_value=f"{TAG_OWNER}"),
                            "tagcomment": struct_pb2.Value(string_value=f"{TAG_COMMENT}"),
                            "tagcreated": struct_pb2.Value(string_value=f"{TAG_CREATED}"),
                            "taglastaltered": struct_pb2.Value(string_value=f"{TAG_LAST_ALTERED}"),
                            "tagallowedvalues": struct_pb2.Value(string_value=f"{TAG_ALLOWED_VALUES}"),
                        }
                    ),
                )
            }
    print(aspects)

    entry = dataplex_v1.Entry()
    entry.name = name
    entry.entry_type = entry_type
    entry.aspects = aspects
    parent = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}"
    print(parent)
    client = dataplex_v1.CatalogServiceClient()
    response = client.create_entry(parent=parent, entry=entry, entry_id=entry_id)
    print("Tag successfully loaded in Dataplex")

# Query 5: TAG REFERENCES
try:
    cursor_tagsref = conn.cursor()
    query_tagsref = "select * from SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES;"
    cursor_tagsref.execute(query_tagsref)

    # Fetch all results and load into a Pandas DataFrame
    df_tagsref = pd.DataFrame(cursor_tagsref.fetchall(), columns=[col[0] for col in cursor_tagsref.description])
    print("\n--- Output for ACCOUNT_USUAGE.TAGSREF ---")
    print(df_tagsref.head()) # Print first few rows
    print(f"Total rows for TAGSREF: {len(df_tagsref)}")

except Exception as e:
    print(f"Error executing TAGSREF query: {e}")
finally:
    if 'cursor_tagsref' in locals() and cursor_tagsref:
        cursor_tagsref.close()

for _, row in df_tagsref.iterrows():
    TAG_NAME = row['TAG_NAME']
    TAG_SCHEMA = row['TAG_SCHEMA']
    TAG_DATABASE = row['TAG_DATABASE']
    TABLE_NAME = row['OBJECT_NAME']
    COLUMN_NAME = row['COLUMN_NAME']
    TAG_VALUE = row['TAG_VALUE']
    ENTRY_ID = f"{TAG_NAME}"[:10]
    ENTRY_ID = f"{ENTRY_ID}{COLUMN_NAME}"
    print(TAG_NAME,TAG_SCHEMA,TAG_DATABASE,TABLE_NAME,COLUMN_NAME,TAG_VALUE)
    entry_id = f"{ENTRY_ID}".replace('.', '-').replace(' ', '_').upper()
    entry_id = entry_id[:40].strip('-').strip('_')
    print(entry_id)

    entry_group_id = "snowflakehorizongrp" #enter your entry group id for snowflake
    entry_type_id = "snowhorizontagref" #enter your entry type id for snowflake tag ref
    name = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}/entries/{entry_id}"
    print(name)
    entry_type = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryTypes/{entry_type_id}"
    print(entry_type)
    aspects={
                f"{PROJECT_ID}.{DATAPLEX_LOCATION}.{entry_type_id}": dataplex_v1.Aspect(
                    aspect_type=f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/aspectTypes/{entry_type_id}",
                    data=struct_pb2.Struct(
                        fields={
                            "tagname": struct_pb2.Value(string_value=f"{TAG_NAME}"),
                            "tagschema": struct_pb2.Value(string_value=f"{TAG_SCHEMA}"),
                            "tagdatabase": struct_pb2.Value(string_value=f"{TAG_DATABASE}"),
                            "tablename": struct_pb2.Value(string_value=f"{TABLE_NAME}"),
                            "columnname": struct_pb2.Value(string_value=f"{COLUMN_NAME}"),
                            "tagvalue": struct_pb2.Value(string_value=f"{TAG_VALUE}"),
                        }
                    ),
                )
            }
    print(aspects)
    entry = dataplex_v1.Entry()
    entry.name = name
    entry.entry_type = entry_type
    entry.aspects = aspects
    parent = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}"
    print(parent)
    client = dataplex_v1.CatalogServiceClient()
    response = client.create_entry(parent=parent, entry=entry, entry_id=entry_id)
    print("Tag successfully loaded in Dataplex")

# Query 6: INDEXES
try:
    cursor_indexes = conn.cursor()
    query_indexes = "select * from snowflake.account_usage.indexes;"
    cursor_indexes.execute(query_indexes)

    # Fetch all results and load into a Pandas DataFrame
    df_indexes = pd.DataFrame(cursor_indexes.fetchall(), columns=[col[0] for col in cursor_indexes.description])
    print("\n--- Output for ACCOUNT_USUAGE.INDEXES ---")
    print(df_indexes.head()) # Print first few rows
    print(f"Total rows for INDEXES: {len(df_indexes)}")

except Exception as e:
    print(f"Error executing INDEXES query: {e}")
finally:
    if 'cursor_indexes' in locals() and cursor_indexes:
        cursor_indexes.close()

for _, row in df_indexes.iterrows():
    INDEX_NAME = row['NAME']
    TABLE_NAME = row['TABLE_NAME']
    SCHEMA_NAME = row['SCHEMA_NAME']
    DATABASE_NAME = row['DATABASE_NAME']
    INDEX_CREATED = str(row['CREATED'])
    print(INDEX_NAME,TABLE_NAME,SCHEMA_NAME,DATABASE_NAME,INDEX_CREATED)
    print(type(INDEX_CREATED))
    entry_id = f"{INDEX_NAME}".replace('.', '-').replace(' ', '_').upper()
    entry_id = entry_id[:40].strip('-').strip('_')
    print(entry_id)

    entry_group_id = "snowflakehorizongrp" #enter your entry group id for snowflake
    entry_type_id = "snowhorizonindex" #enter your entry type id for snowflake indexes
    name = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}/entries/{entry_id}"
    print(name)
    entry_type = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryTypes/{entry_type_id}"
    print(entry_type)
    aspects={
                f"{PROJECT_ID}.{DATAPLEX_LOCATION}.{entry_type_id}": dataplex_v1.Aspect(
                    aspect_type=f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/aspectTypes/{entry_type_id}",
                    data=struct_pb2.Struct(
                        fields={
                            "indexname": struct_pb2.Value(string_value=f"{INDEX_NAME}"),
                            "tablename": struct_pb2.Value(string_value=f"{TABLE_NAME}"),
                            "schemaname": struct_pb2.Value(string_value=f"{SCHEMA_NAME}"),
                            "databasename": struct_pb2.Value(string_value=f"{DATABASE_NAME}"),
                            "indexcreated": struct_pb2.Value(string_value=f"{INDEX_CREATED}"),
                        }
                    ),
                )
            }
    print(aspects)

    entry = dataplex_v1.Entry()
    entry.name = name
    entry.entry_type = entry_type
    entry.aspects = aspects
    parent = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}"
    print(parent)
    client = dataplex_v1.CatalogServiceClient()
    response = client.create_entry(parent=parent, entry=entry, entry_id=entry_id)
    print("Index successfully loaded in Dataplex")

# Query 7: FUNCTIONS
try:
    cursor_function = conn.cursor()
    query_function = "select * from snowflake.account_usage.functions;"
    cursor_function.execute(query_function)

    # Fetch all results and load into a Pandas DataFrame
    df_function = pd.DataFrame(cursor_function.fetchall(), columns=[col[0] for col in cursor_function.description])
    print("\n--- Output for ACCOUNT_USUAGE.FUNCTIONS ---")
    print(df_function.head()) # Print first few rows
    print(f"Total rows for FUNCTIONS: {len(df_function)}")

except Exception as e:
    print(f"Error executing FUNCTIONS query: {e}")
finally:
    if 'cursor_function' in locals() and cursor_function:
        cursor_function.close()

for _, row in df_function.iterrows():
    FUNCTION_NAME = row['FUNCTION_NAME']
    FUNCTION_SCHEMA = row['FUNCTION_SCHEMA']
    FUNCTION_CATALOG = row['FUNCTION_CATALOG']
    FUNCTION_OWNER = row['FUNCTION_OWNER']
    ARGUMENT_SIGNATURE = str(row['ARGUMENT_SIGNATURE'])
    DATA_TYPE = str(row['DATA_TYPE'])
    FUNCTION_LANGUAGE = row['FUNCTION_LANGUAGE']
    FUNCTION_DEFINITION = str(row['FUNCTION_DEFINITION'])
    FUNCTION_CREATED = str(row['CREATED'])
    FUNCTION_LAST_ALTERED = str(row['LAST_ALTERED'])
    FUNCTION_COMMENT = row['COMMENT']
    print(FUNCTION_NAME,FUNCTION_SCHEMA,FUNCTION_CATALOG,FUNCTION_OWNER,ARGUMENT_SIGNATURE,DATA_TYPE,FUNCTION_LANGUAGE,FUNCTION_DEFINITION,FUNCTION_CREATED,FUNCTION_LAST_ALTERED,FUNCTION_COMMENT)
    print(type(FUNCTION_CREATED))
    entry_id = f"{FUNCTION_NAME}".replace('.', '-').replace(' ', '_').upper()
    entry_id = entry_id[:40].strip('-').strip('_')
    print(entry_id)

    entry_group_id = "snowflakehorizongrp" #enter your entry group id for snowflake
    entry_type_id = "snowhorizonfunction" #enter your entry type id for snowflake function
    name = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}/entries/{entry_id}"
    print(name)
    entry_type = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryTypes/{entry_type_id}"
    print(entry_type)
    aspects={
                f"{PROJECT_ID}.{DATAPLEX_LOCATION}.{entry_type_id}": dataplex_v1.Aspect(
                    aspect_type=f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/aspectTypes/{entry_type_id}",
                    data=struct_pb2.Struct(
                        fields={
                            "functionname": struct_pb2.Value(string_value=f"{FUNCTION_NAME}"),
                            "functionschema": struct_pb2.Value(string_value=f"{FUNCTION_SCHEMA}"),
                            "functioncatalog": struct_pb2.Value(string_value=f"{FUNCTION_CATALOG}"),
                            "functionowner": struct_pb2.Value(string_value=f"{FUNCTION_OWNER}"),
                            "argumentsignature": struct_pb2.Value(string_value=f"{ARGUMENT_SIGNATURE}"),
                            "datatype": struct_pb2.Value(string_value=f"{DATA_TYPE}"),
                            "functionlanguage": struct_pb2.Value(string_value=f"{FUNCTION_LANGUAGE}"),
                            "functiondefinition": struct_pb2.Value(string_value=f"{FUNCTION_DEFINITION}"),
                            "functioncreated": struct_pb2.Value(string_value=f"{FUNCTION_CREATED}"),
                            "functionlastaltered": struct_pb2.Value(string_value=f"{FUNCTION_LAST_ALTERED}"),
                            "functioncomment": struct_pb2.Value(string_value=f"{FUNCTION_COMMENT}"),
                        }
                    ),
                )
            }
    print(aspects)

    entry = dataplex_v1.Entry()
    entry.name = name
    entry.entry_type = entry_type
    entry.aspects = aspects
    parent = f"projects/{PROJECT_ID}/locations/{DATAPLEX_LOCATION}/entryGroups/{entry_group_id}"
    print(parent)
    client = dataplex_v1.CatalogServiceClient()
    response = client.create_entry(parent=parent, entry=entry, entry_id=entry_id)
    print("Function successfully loaded in Dataplex")

#all done