# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7fa7e213-3f48-43a9-9854-240338af99f8",
# META       "default_lakehouse_name": "lh_Main",
# META       "default_lakehouse_workspace_id": "4dde6d65-494a-490e-895a-613e38da7758",
# META       "known_lakehouses": [
# META         {
# META           "id": "7fa7e213-3f48-43a9-9854-240338af99f8"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # control table to register schema drifts

# CELL ********************

if not spark.catalog.tableExists("dbo.schema_drift_log"):
    spark.sql("""
        CREATE TABLE dbo.schema_drift_log (
            source_entity STRING,
            check_timestamp TIMESTAMP,
            new_fields ARRAY<STRING>,
            missing_fields ARRAY<STRING>,
            sample_row_count INT
        ) USING DELTA
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, ArrayType
from datetime import datetime

def _flatten_field_paths(dtype, prefix=""):
    """Recursively walk a StructType/ArrayType, returning a set of dotted field paths."""
    paths = set()
    if isinstance(dtype, StructType):
        for f in dtype.fields:
            full_path = f"{prefix}{f.name}"
            paths.add(full_path)
            paths |= _flatten_field_paths(f.dataType, f"{full_path}.")
    elif isinstance(dtype, ArrayType):
        # arrays of structs: descend without adding an index segment
        paths |= _flatten_field_paths(dtype.elementType, prefix)
    return paths


def check_schema_drift(bronze_df, raw_col, expected_schema, source_entity, sample_size=200):
    """
    Compares the field paths of `expected_schema` (the StructType used in your
    from_json call) against what Spark infers from a sample of actual raw JSON.
    Logs any discrepancy to dbo.schema_drift_log. Returns (new_fields, missing_fields).
    """
    sample_rows = (
        bronze_df
        .orderBy(F.col("_ingested_at").desc())
        .limit(sample_size)
        .select(raw_col)
        .rdd.map(lambda r: r[raw_col])
    )

    if sample_rows.isEmpty():
        return set(), set()  # nothing to check yet

    inferred_schema = spark.read.json(sample_rows).schema

    expected_paths = _flatten_field_paths(expected_schema)
    actual_paths = _flatten_field_paths(inferred_schema)

    new_fields = actual_paths - expected_paths
    missing_fields = expected_paths - actual_paths

    log_row = spark.createDataFrame(
        [(source_entity, datetime.now(), sorted(new_fields), sorted(missing_fields), sample_size)],
        schema="source_entity STRING, check_timestamp TIMESTAMP, new_fields ARRAY<STRING>, missing_fields ARRAY<STRING>, sample_row_count INT"
    )
    log_row.write.format("delta").mode("append").saveAsTable("dbo.schema_drift_log")

    return new_fields, missing_fields

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
