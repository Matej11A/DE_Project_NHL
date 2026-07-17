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

# # extract from bronze.dim_players creating a fact table with 2 rows per one player with their stats

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

career_totals_struct = StructType([
    StructField("assists", IntegerType(), True),
    StructField("avgToi", StringType(), True),
    StructField("faceoffWinningPctg", DoubleType(), True),
    StructField("gameWinningGoals", IntegerType(), True),
    StructField("gamesPlayed", IntegerType(), True),
    StructField("gamesStarted", IntegerType(), True),
    StructField("goals", IntegerType(), True),
    StructField("goalsAgainst", IntegerType(), True),
    StructField("goalsAgainstAvg", DoubleType(), True),
    StructField("losses", IntegerType(), True),
    StructField("otGoals", IntegerType(), True),
    StructField("otLosses", IntegerType(), True),
    StructField("pim", IntegerType(), True),
    StructField("plusMinus", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("powerPlayGoals", IntegerType(), True),
    StructField("powerPlayPoints", IntegerType(), True),
    StructField("savePctg", DoubleType(), True),
    StructField("shootingPctg", DoubleType(), True),
    StructField("shorthandedGoals", IntegerType(), True),
    StructField("shorthandedPoints", IntegerType(), True),
    StructField("shots", IntegerType(), True),
    StructField("shotsAgainst", IntegerType(), True),
    StructField("shutouts", IntegerType(), True),
    StructField("timeOnIce", StringType(), True),
    StructField("wins", IntegerType(), True)
])

player_schema = StructType([
    StructField("playerId", StringType(), False),
    StructField("careerTotals", StructType([
        StructField("regularSeason", career_totals_struct, True),
        StructField("playoffs", career_totals_struct, True)
    ]), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.read.table("bronze.dim_players")

df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), player_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

regular_season = df_parsed.select(
    F.col("parsed.playerId").alias("player_id"),
    F.lit("regularSeason").alias("game_type"),
    F.col("parsed.careerTotals.regularSeason.*")
)

playoffs = df_parsed.select(
    F.col("parsed.playerId").alias("player_id"),
    F.lit("playoffs").alias("game_type"),
    F.col("parsed.careerTotals.playoffs.*")
)

df_fact_career_totals = regular_season.unionByName(playoffs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# sanity check

# CELL ********************

df_fact_career_totals.show(10, truncate=False)
df_fact_career_totals.printSchema()
print(f"Row count: {df_fact_career_totals.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_career_totals.write.format("delta").mode("overwrite").saveAsTable("silver.fact_player_career_totals")

print(f"Saved silver.fact_player_career_totals with {df_fact_career_totals.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
