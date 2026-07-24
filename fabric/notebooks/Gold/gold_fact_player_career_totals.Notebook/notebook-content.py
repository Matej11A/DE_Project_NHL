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

silver_career_totals = spark.table("silver.fact_player_career_totals")
gold_players_current = spark.table("gold.dim_players").filter(F.col("is_current") == True)

players_dim = gold_players_current.alias("p")

df_career_totals = (
    silver_career_totals.alias("c")
    .join(
        players_dim,
        on=F.col("c.player_id") == F.col("p.player_id"),
        how="left"
    )
    .select(
        F.col("p.player_sk").alias("player_sk"),
        "c.player_id",
        "c.game_type",
        "c.assists", "c.avgToi", "c.faceoffWinningPctg", "c.gameWinningGoals",
        "c.gamesPlayed", "c.gamesStarted", "c.goals", "c.goalsAgainst",
        "c.goalsAgainstAvg", "c.losses", "c.otGoals", "c.otLosses", "c.pim",
        "c.plusMinus", "c.points", "c.powerPlayGoals", "c.powerPlayPoints",
        "c.savePctg", "c.shootingPctg", "c.shorthandedGoals", "c.shorthandedPoints",
        "c.shots", "c.shotsAgainst", "c.shutouts", "c.timeOnIce", "c.wins",
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_career_totals.write.format("delta").mode("overwrite").saveAsTable("gold.fact_player_career_totals")
print(f"Table gold.fact_player_career_totals saved with {df_career_totals.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
