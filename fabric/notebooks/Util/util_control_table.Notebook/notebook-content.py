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

# # utility notebook that creates a control table for watermarks 

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

control_schema = StructType([
    StructField("source_entity", StringType(), False),
    StructField("last_watermark_value", StringType(), True),
    StructField("last_run_timestamp", TimestampType(), True),
    StructField("rows_processed_last_run", IntegerType(), True),
])

table_exists = spark.catalog.tableExists("dbo.control_table")

if not table_exists:
    empty_df = spark.createDataFrame([], schema=control_schema)
    empty_df.write.format("delta").saveAsTable("dbo.control_table")
    print("Created dbo.control_table")
else:
    print("dbo.control_table already exists - skipping creation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
