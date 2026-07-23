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

# # notebook to flatten bronze.dim_nhl_teams and correctly cast data types

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, TimestampType, LongType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.read.table("bronze.dim_nhl_teams")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_teams = df_bronze.select(
    F.col("franchise_id").cast(LongType()),
    F.col("name").cast(StringType()),
    F.col("common_name").cast(StringType()),
    F.col("abbr").cast(StringType()),
    F.col("logo").cast(StringType()),
    F.col("conference")["name"].cast(StringType()).alias("conference_name"),
    F.col("conference")["abbr"].cast(StringType()).alias("conference_abbr"),
    F.col("division")["name"].cast(StringType()).alias("division_name"),
    F.col("division")["abbr"].cast(StringType()).alias("division_abbr")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_teams.write.format("delta").mode("overwrite").saveAsTable("silver.dim_nhl_teams")

print(f"Table successfully saved as silver.dim_nhl_teams with {df_dim_teams.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
