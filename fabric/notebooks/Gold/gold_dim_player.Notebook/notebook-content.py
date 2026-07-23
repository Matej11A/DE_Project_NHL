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

# # notebook to bring few columns from silver.dim_NHL_teams to dim_player and create an unified table

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_dim_players = spark.table("silver.dim_players")
silver_dim_teams = spark.table("silver.dim_nhl_teams")


gold_dim_player = (
    silver_dim_players
    .join(
        silver_dim_teams.select(
            F.col("franchise_id"),
            F.col("name"),
            F.col("conference_name"),
            F.col("division_name")
        ),
        on="franchise_id",
        how="left"
    )
    .withColumn("player_sk", F.xxhash64(F.col("player_id"), F.col("effective_start_date")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dim_player.write.format("delta").mode("overwrite").saveAsTable("gold.dim_players")
print(f"Table gold.dim_players saved with {gold_dim_player.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
