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

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_team_df = spark.read.table("silver.dim_nhl_teams")

df_dim_nhl_teams = (
    silver_team_df
    .withColumn("team_sk", F.xxhash64("franchise_id"))
    .select(
        "team_sk",
        "franchise_id",
        "name",
        "abbr",
        "logo",
        "conference_name",
        "division_name"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_nhl_teams.write.format("delta").mode("overwrite").saveAsTable("gold.dim_nhl_teams")
print(f"Table gold.dim_nhl_teams saved with {df_dim_nhl_teams.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
