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


@F.udf(returnType=StringType())
def choose_metadata_type_udf(data_type: str):
    """Choose the dataplex metadata type based on native source type."""
    return get_catalog_metadata_type(data_type)


def create_entry_source(column):
    """Create Entry Source segment."""
    return F.named_struct(F.lit("display_name"),
                          column,
                          F.lit("system"),
                          F.lit(SOURCE_TYPE))


def create_entry_aspect(entry_aspect_name):
    """Create aspect with general information (usually it is empty)."""
    return F.create_map(
        F.lit(entry_aspect_name),
        F.named_struct(
            F.lit("aspect_type"),
            F.lit(entry_aspect_name),
            F.lit("data"),
            F.create_map()
            )
        )


def convert_to_import_items(df, aspect_keys):
    """Convert entries to import items."""
    entry_columns = ["name", "fully_qualified_name", "parent_entry",
                     "entry_source", "aspects", "entry_type"]

    # Puts entry to "entry" key, a list of keys from aspects in "aspects_keys"
    # and "aspects" string in "update_mask"
    return df.withColumn("entry", F.struct(entry_columns)) \
      .withColumn("aspect_keys", F.array([F.lit(key) for key in aspect_keys])) \
      .withColumn("update_mask", F.array(F.lit("aspects"))) \
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

    # Fills the missed project and location into the entry type string
    full_entry_type = entry_type.value.format(
        project=config["target_project_id"],
        location=config["target_location_id"])

    # Converts a list of schema names to the Dataplex-compatible form
    column = F.col("SCHEMA_NAME")
    df = df_raw_schemas.withColumn("name", create_name_udf(column)) \
      .withColumn("fully_qualified_name", create_fqn_udf(column)) \
      .withColumn("parent_entry", F.lit(parent_name)) \
      .withColumn("entry_type", F.lit(full_entry_type)) \
      .withColumn("entry_source", create_entry_source(column)) \
      .withColumn("aspects", create_entry_aspect(entry_aspect_name)) \
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
      .withColumn("mode", F.when(F.col("IS_NULLABLE") == True, "NULLABLE").otherwise("REQUIRED")) \
        .drop("IS_NULLABLE") \
        .withColumnRenamed("DATA_TYPE", "dataType") \
        .withColumn("metadataType", choose_metadata_type_udf("dataType")) \
        .withColumnRenamed("COLUMN_NAME", "name")

    # The transformation below aggregate fields, denormalizing the table
    # TABLE_NAME becomes top-level filed, and the rest is put into
    # the array type called "fields"
    aspect_columns = ["name", "mode", "dataType", "metadataType"]
    df = df.withColumn("columns", F.struct(aspect_columns)) \
      .groupby('TABLE_NAME') \
      .agg(F.collect_list("columns").alias("fields"))

    # Create nested structured called aspects.
    # Fields are becoming a part of a `schema` struct
    # There is also an entry_aspect that is repeats entry_type as aspect_type
    entry_aspect_name = nb.create_entry_aspect_name(config, entry_type)
    df = df.withColumn("schema",
                       F.create_map(F.lit(schema_key),
                                    F.named_struct(
                                        F.lit("aspect_type"),
                                        F.lit(schema_key),
                                        F.lit("data"),
                                        F.create_map(F.lit("fields"),
                                                     F.col("fields")))\
                                    )\
                       )\
      .withColumn("entry_aspect", create_entry_aspect(entry_aspect_name)) \
    .drop("fields")

    # Merge separate aspect columns into the one map called 'aspects'
    df = df.select(F.col("TABLE_NAME"),
                   F.map_concat("schema", "entry_aspect").alias("aspects"))

    # Define user-defined functions to fill the general information
    # and hierarchy names
    create_name_udf = F.udf(lambda x: nb.create_name(config, entry_type,
                                                     db_schema, x),
                            StringType())

    create_fqn_udf = F.udf(lambda x: nb.create_fqn(config, entry_type,
                                                   db_schema, x), StringType())

    parent_name = nb.create_parent_name(entry_type, db_schema)
    full_entry_type = entry_type.value.format(
        project=config["target_project_id"],
        location=config["target_location_id"])

    # Fill the top-level fields
    column = F.col("TABLE_NAME")

    df = df.withColumn("name", create_name_udf(column)) \
      .withColumn("fully_qualified_name", create_fqn_udf(column)) \
      .withColumn("entry_type", F.lit(full_entry_type)) \
      .withColumn("parent_entry", F.lit(parent_name)) \
      .withColumn("entry_source", create_entry_source(column)) \
    .drop(column)

    df = convert_to_import_items(df, [schema_key, entry_aspect_name])
    return df
