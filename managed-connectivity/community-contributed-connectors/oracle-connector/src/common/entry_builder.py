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

"""Creates entries with PySpark."""
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from src.datatype_mapper import get_catalog_metadata_type
from src.constants import SOURCE_TYPE
from src.constants import COLLECTION_ENTRY
from src.constants import EntryType
from src import name_builder as nb
from enum import Enum

# DB-specific value which indicates true
from src.constants import IS_NULLABLE_TRUE

# Property names from google.cloud.dataplex_v1.types.Entry in camel Case
class JSONKeys(Enum):
    NAME = 'name'
    MODE = 'mode'
    ENTRY = 'entry'
    ENTRY_TYPE = 'entryType'
    ENTRY_SOURCE = 'entrySource'
    ASPECT_KEYS = 'aspectKeys'
    ASPECT_TYPE = 'aspectType'
    DISPLAY_NAME = 'displayName'
    UPDATE_MASK = 'updateMask'
    FQN = 'fullyQualifiedName'
    PARENT_ENTRY = 'parentEntry'
    ASPECTS = 'aspects'
    DATA = 'data'
    DATA_TYPE = 'dataType'
    METADATA_TYPE = 'metadataType'
    DESCRIPTION = 'description'
    ENTRY_ASPECT = 'entry_aspect'
    FIELDS = 'fields'
    SYSTEM = 'system'
    PLATFORM = 'platform'
    SCHEMA = 'schema'
    COLUMNS = 'columns'
    DEFAULT_VALUE = 'default'

"""Enum representing the Spark dataframe columns"""
class Columns(Enum):
    TABLE_NAME = 'TABLE_NAME'
    DATA_TYPE = 'DATA_TYPE'
    COLUMN_NAME = 'COLUMN_NAME'
    IS_NULLABLE = 'IS_NULLABLE'
    SCHEMA_NAME = 'SCHEMA_NAME'
    COLUMN_COMMENT = 'COLUMN_COMMENT'
    TABLE_COMMENT = 'TABLE_COMMENT'
    COLUMN_DEFAULT_VALUE = 'DATA_DEFAULT'

# Dataplex constants
class DataplexTypesSchema(Enum):
    NULLABLE = 'NULLABLE'
    REQUIRED = 'REQUIRED'

# universal catalog system AspectType for database tables and schemas
SCHEMA_KEY = "dataplex-types.global.schema"

@F.udf(returnType=StringType())
def choose_metadata_type_udf(data_type: str):
    """Choose the dataplex metadata type based on native source type."""
    return get_catalog_metadata_type(data_type)


def create_entry_source(column,entryType : EntryType,comment):
    """Create Entry Source segment."""

    ## Add comments to description field for tables and views 
    if entryType in [EntryType.TABLE, EntryType.VIEW]:
        return F.named_struct(F.lit(JSONKeys.DISPLAY_NAME.value),
                          column,
                          F.lit(JSONKeys.SYSTEM.value),
                          F.lit(SOURCE_TYPE),
                          F.lit(JSONKeys.DESCRIPTION.value),
                          F.lit(comment)
                          )
    else:
        return F.named_struct(F.lit(JSONKeys.DISPLAY_NAME.value),
                          column,
                          F.lit(JSONKeys.SYSTEM.value),
                          F.lit(SOURCE_TYPE)
                          )


def create_entry_aspect(entry_aspect_name):
    """Create aspect with general information (usually it is empty)."""
    return F.create_map(
        F.lit(entry_aspect_name),
        F.named_struct(
            F.lit(JSONKeys.ASPECT_TYPE.value),
            F.lit(entry_aspect_name),
            F.lit(JSONKeys.DATA.value),
            F.create_map()
            )
        )


def convert_to_import_items(df, aspect_keys):
    """Convert entries to import items."""
    entry_columns = [JSONKeys.NAME.value, JSONKeys.FQN.value, JSONKeys.PARENT_ENTRY.value,
                     JSONKeys.ENTRY_SOURCE.value, JSONKeys.ASPECTS.value, JSONKeys.ENTRY_TYPE.value]

    # Puts entry to "entry" key, a list of keys from aspects in "aspects_keys"
    # and "aspects" string in "update_mask"
    return df.withColumn(JSONKeys.ENTRY.value, F.struct(entry_columns)) \
      .withColumn(JSONKeys.ASPECT_KEYS.value, F.array([F.lit(key) for key in aspect_keys])) \
      .withColumn(JSONKeys.UPDATE_MASK.value, F.array(F.lit(JSONKeys.ASPECTS.value))) \
      .drop(*entry_columns)


def build_schemas(config, df_raw_schemas):
    """Create a dataframe with database schemas from the list of usernames.
    Args:
        df_raw_schemas - a dataframe with only one column called SCHEMA_NAME
    Returns:
        A dataframe with Dataplex-readable schemas.
    """
    entry_type = COLLECTION_ENTRY
    entry_aspect_name = nb.create_entry_aspect_name(config, entry_type)

    # For schema, parent name is the name of the database
    parent_name =  nb.create_parent_name(config, entry_type)

    # Create user-defined function.
    create_name_udf = F.udf(lambda x: nb.create_name(config, entry_type, x),
                            StringType())
    create_fqn_udf = F.udf(lambda x: nb.create_fqn(config, entry_type, x),
                           StringType())

    # Fills the project and location into the entry type string
    full_entry_type = entry_type.value.format(
        project=config["target_project_id"],
        location=config["target_location_id"])

    # Convert list of schema names to Dataplex-compatible form

    column = F.col(Columns.SCHEMA_NAME.value)
    df = df_raw_schemas.withColumn(JSONKeys.NAME.value, create_name_udf(column)) \
      .withColumn(JSONKeys.FQN.value, create_fqn_udf(column)) \
      .withColumn(JSONKeys.PARENT_ENTRY.value, F.lit(parent_name)) \
      .withColumn(JSONKeys.ENTRY_TYPE.value, F.lit(full_entry_type)) \
      .withColumn(JSONKeys.ENTRY_SOURCE.value, create_entry_source(column,entry_type,F.col(JSONKeys.DESCRIPTION.value))) \
      .withColumn(JSONKeys.ASPECTS.value, create_entry_aspect(entry_aspect_name)) \
    .drop(column)

    df = convert_to_import_items(df, [entry_aspect_name])
    return df


def build_dataset(config, df_raw, db_schema, entry_type):
    """Build table entries from a flat list of columns.
    Args:
        df_raw - a plain dataframe with TABLE_NAME, COLUMN_NAME, DATA_TYPE,NULLABLE,COMMENT columns
        db_schema - parent database schema
        entry_type - entry type: table or view
    Returns:
        A dataframe with Dataplex-readable data of tables of views.
    """

    # The transformation below does the following
    # 1. Alters IS_NULLABLE content from 1/0 to NULLABLE/REQUIRED
    # 2. Renames IS_NULLABLE to mode
    # 3. Creates metadataType column based on dataType column
    # 4. Renames COLUMN_NAME to name
    # 5. Renames COMMENT to DESCRIPTION
    # 6. Renames DATA_DEFAULT to DEFAULT_VALUE

    print(f"BUILD_DATASET 1: {df_raw.show(n=5)}")    

    df = df_raw \
        .withColumn(JSONKeys.MODE.value, F.when(F.col(Columns.IS_NULLABLE.value) == IS_NULLABLE_TRUE, DataplexTypesSchema.NULLABLE.value).otherwise(DataplexTypesSchema.REQUIRED.value)) \
        .drop(Columns.IS_NULLABLE.value) \
        .withColumnRenamed(Columns.DATA_TYPE.value, JSONKeys.DATA_TYPE.value) \
        .withColumn(JSONKeys.METADATA_TYPE.value, choose_metadata_type_udf(JSONKeys.DATA_TYPE.value)) \
        .withColumnRenamed(Columns.COLUMN_NAME.value, JSONKeys.NAME.value) \
        .withColumnRenamed(Columns.COLUMN_COMMENT.value, JSONKeys.DESCRIPTION.value) \
        .withColumnRenamed(Columns.COLUMN_DEFAULT_VALUE.value, JSONKeys.DEFAULT_VALUE.value) \
        .na.fill(value='',subset=[JSONKeys.DESCRIPTION.value]) \
        .na.fill(value='',subset=[Columns.TABLE_COMMENT.value])
    
    print(f"BUILD_DATASET 2: {df.show(n=5)}")  

    # Transformation to aggregates fields, denormalizing the table
    # TABLE_NAME becomes top-level field, rest are put into array type "fields"
    aspect_columns = [JSONKeys.NAME.value, JSONKeys.MODE.value, JSONKeys.DATA_TYPE.value, JSONKeys.METADATA_TYPE.value, JSONKeys.DESCRIPTION.value]
    df = df.withColumn(JSONKeys.COLUMNS.value, F.struct(aspect_columns)) \
      .groupby(Columns.TABLE_NAME.value, Columns.TABLE_COMMENT.value) \
      .agg(F.collect_list(JSONKeys.COLUMNS.value).alias(JSONKeys.FIELDS.value))

    df = df.withColumnRenamed(Columns.TABLE_COMMENT.value, JSONKeys.DESCRIPTION.value) 

    # Create nested structured called aspects.
    # Fields becoming part of the 'schema' struct
    # Entry_aspect repeats each entry_type for the aspect_type
    entry_aspect_name = nb.create_entry_aspect_name(config, entry_type)
    df = df.withColumn(JSONKeys.SCHEMA.value,
                       F.create_map(F.lit(SCHEMA_KEY),
                                    F.named_struct(
                                        F.lit(JSONKeys.ASPECT_TYPE.value),
                                        F.lit(SCHEMA_KEY),
                                        F.lit(JSONKeys.DATA.value),
                                        F.create_map(F.lit(JSONKeys.FIELDS.value),
                                                     F.col(JSONKeys.FIELDS.value)))\
                                    )\
                       )\
      .withColumn(JSONKeys.ENTRY_ASPECT.value, create_entry_aspect(entry_aspect_name)) \
      .drop(JSONKeys.FIELDS.value)
      

    # Merge separate aspect columns into 'aspects' map
    df = df.select(F.col(Columns.TABLE_NAME.value),F.col(JSONKeys.DESCRIPTION.value),F.col(JSONKeys.DEFAULT_VALUE.value),
                   F.map_concat(JSONKeys.SCHEMA.value, JSONKeys.ENTRY_ASPECT.value).alias(JSONKeys.ASPECTS.value))
    
    print(f"2. df = {df.show()}")

    # Define user-defined functions to fill the general information
    # and hierarchy names
    create_name_udf = F.udf(lambda x: nb.create_name(config, entry_type,
                                                     db_schema, x),
                            StringType())

    create_fqn_udf = F.udf(lambda x: nb.create_fqn(config, entry_type,
                                                   db_schema, x), StringType())

    parent_name = nb.create_parent_name(config,entry_type, db_schema)

    full_entry_type = entry_type.value.format(
        project=config["target_project_id"],
        location=config["target_location_id"])

    # Fill the top-level fields
    column = F.col(Columns.TABLE_NAME.value)

    df = df.withColumn(JSONKeys.NAME.value, create_name_udf(column)) \
      .withColumn(JSONKeys.FQN.value, create_fqn_udf(column)) \
      .withColumn(JSONKeys.ENTRY_TYPE.value, F.lit(full_entry_type)) \
      .withColumn(JSONKeys.PARENT_ENTRY.value, F.lit(parent_name)) \
      .withColumn(JSONKeys.ENTRY_SOURCE.value, create_entry_source(column,entry_type,F.col(JSONKeys.DESCRIPTION.value))) \
    .drop(column) \
    .drop(JSONKeys.DESCRIPTION.value)

    df = convert_to_import_items(df, [SCHEMA_KEY, entry_aspect_name])

    return df