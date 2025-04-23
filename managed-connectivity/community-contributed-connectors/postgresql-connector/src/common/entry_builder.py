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
from src import name_builder as nb

# DB-specific value which indicates true
from src.constants import IS_NULLABLE_TRUE

# Property names from Dataplex_v1.Entry object in Camel Case
KEY_NAME = "name"
KEY_MODE = "mode"
KEY_ENTRY = "entry"
KEY_ENTRY_TYPE = "entryType"
KEY_ENTRY_SOURCE = "entrySource"
KEY_ASPECT_KEYS = "aspectKeys"
KEY_ASPECT_TYPE = "aspectType"
KEY_DISPLAY_NAME = "displayName"
KEY_UPDATE_MASK = "updateMask"
KEY_FQN = "fullyQualifiedName"
KEY_PARENT_ENTRY = "parentEntry"
KEY_ASPECTS = "aspects"
KEY_DATA = "data"
KEY_DATA_TYPE = "dataType"
KEY_METADATA_TYPE = "metadataType"

KEY_ENTRY_ASPECT = "entry_aspect"

KEY_FIELDS = "fields"
KEY_SYSTEM = "system"
KEY_SCHEMA = "schema"

KEY_COLUMNS = "columns"

COLUMN_TABLE_NAME = "TABLE_NAME"
COLUMN_DATA_TYPE = "DATA_TYPE"
COLUMN_COLUMN_NAME = "COLUMN_NAME"
COLUMN_IS_NULLABLE = "IS_NULLABLE"
COLUMN_SCHEMA_NAME = "SCHEMA_NAME"

VALUE_NULLABLE = "NULLABLE"
VALUE_REQUIRED = "REQUIRED"

@F.udf(returnType=StringType())
def choose_metadata_type_udf(data_type: str):
    """Choose the dataplex metadata type based on native source type."""
    return get_catalog_metadata_type(data_type)


def create_entry_source(column):
    """Create Entry Source segment."""
    return F.named_struct(F.lit(KEY_DISPLAY_NAME),
                          column,
                          F.lit(KEY_SYSTEM),
                          F.lit(SOURCE_TYPE))


def create_entry_aspect(entry_aspect_name):
    """Create aspect with general information (usually it is empty)."""
    return F.create_map(
        F.lit(entry_aspect_name),
        F.named_struct(
            F.lit(KEY_ASPECT_TYPE),
            F.lit(entry_aspect_name),
            F.lit(KEY_DATA),
            F.create_map()
            )
        )


def convert_to_import_items(df, aspect_keys):
    """Convert entries to import items."""
    entry_columns = [KEY_NAME, KEY_FQN, KEY_PARENT_ENTRY,
                     KEY_ENTRY_SOURCE, KEY_ASPECTS, KEY_ENTRY_TYPE]

    # Puts entry to "entry" key, a list of keys from aspects in "aspects_keys"
    # and "aspects" string in "update_mask"
    return df.withColumn(KEY_ENTRY, F.struct(entry_columns)) \
      .withColumn(KEY_ASPECT_KEYS, F.array([F.lit(key) for key in aspect_keys])) \
      .withColumn(KEY_UPDATE_MASK, F.array(F.lit(KEY_ASPECTS))) \
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

    # Converts a list of schema names to the Dataplex-compatible form
    column = F.col(COLUMN_SCHEMA_NAME)
    df = df_raw_schemas.withColumn(KEY_NAME, create_name_udf(column)) \
      .withColumn(KEY_FQN, create_fqn_udf(column)) \
      .withColumn(KEY_PARENT_ENTRY, F.lit(parent_name)) \
      .withColumn(KEY_ENTRY_TYPE, F.lit(full_entry_type)) \
      .withColumn(KEY_ENTRY_SOURCE, create_entry_source(column)) \
      .withColumn(KEY_ASPECTS, create_entry_aspect(entry_aspect_name)) \
    .drop(column)

    df = convert_to_import_items(df, [entry_aspect_name])
    return df


def build_dataset(config, df_raw, db_schema, entry_type):
    """Build table entries from a flat list of columns.
    Args:
        df_raw - a plain dataframe with TABLE_NAME, COLUMN_NAME, DATA_TYPE,
                 and NULLABLE columns
        db_schema - parent database schema
        entry_type - entry type: table or view
    Returns:
        A dataframe with Dataplex-readable data of tables of views.
    """
    schema_key = "dataplex-types.global.schema"

    # The transformation below does the following
    # 1. Alters IS_NULLABLE content from 1/0 to NULLABLE/REQUIRED
    # 2. Renames IS_NULLABLE to mode
    # 3. Creates metadataType column based on dataType column
    # 4. Renames COLUMN_NAME to name
    df = df_raw \
      .withColumn(KEY_MODE, F.when(F.col(COLUMN_IS_NULLABLE) == IS_NULLABLE_TRUE, VALUE_NULLABLE).otherwise(VALUE_REQUIRED)) \
        .drop(COLUMN_IS_NULLABLE) \
        .withColumnRenamed(COLUMN_DATA_TYPE, KEY_DATA_TYPE) \
        .withColumn(KEY_METADATA_TYPE, choose_metadata_type_udf(KEY_DATA_TYPE)) \
        .withColumnRenamed(COLUMN_COLUMN_NAME, KEY_NAME)

    # The transformation below aggregates fields, denormalizing the table
    # TABLE_NAME becomes top-level filed, and the rest is put into
    # the array type called "fields"
    aspect_columns = [KEY_NAME, KEY_MODE, KEY_DATA_TYPE, KEY_METADATA_TYPE]
    df = df.withColumn(KEY_COLUMNS, F.struct(aspect_columns)) \
      .groupby(COLUMN_TABLE_NAME) \
      .agg(F.collect_list(KEY_COLUMNS).alias(KEY_FIELDS))

    # Create nested structured called aspects.
    # Fields are becoming a part of a `schema` struct
    # There is also an entry_aspect that is repeats entry_type as aspect_type
    entry_aspect_name = nb.create_entry_aspect_name(config, entry_type)
    df = df.withColumn(KEY_SCHEMA,
                       F.create_map(F.lit(schema_key),
                                    F.named_struct(
                                        F.lit(KEY_ASPECT_TYPE),
                                        F.lit(schema_key),
                                        F.lit(KEY_DATA),
                                        F.create_map(F.lit(KEY_FIELDS),
                                                     F.col(KEY_FIELDS)))\
                                    )\
                       )\
      .withColumn(KEY_ENTRY_ASPECT, create_entry_aspect(entry_aspect_name)) \
    .drop(KEY_FIELDS)

    # Merge separate aspect columns into the one map called 'aspects'
    df = df.select(F.col(COLUMN_TABLE_NAME),
                   F.map_concat(KEY_SCHEMA, KEY_ENTRY_ASPECT).alias(KEY_ASPECTS))

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
    column = F.col(COLUMN_TABLE_NAME)

    df = df.withColumn(KEY_NAME, create_name_udf(column)) \
      .withColumn(KEY_FQN, create_fqn_udf(column)) \
      .withColumn(KEY_ENTRY_TYPE, F.lit(full_entry_type)) \
      .withColumn(KEY_PARENT_ENTRY, F.lit(parent_name)) \
      .withColumn(KEY_ENTRY_SOURCE, create_entry_source(column)) \
    .drop(column)

    df = convert_to_import_items(df, [schema_key, entry_aspect_name])
    return df
