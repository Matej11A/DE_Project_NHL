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

# # defining scalar values from EDGE stats for goalies

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, LongType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# schema definiton for parsing

struct_goal = StructType([
    StructField("value", DoubleType(), True),
    StructField("percentile", DoubleType(), True),
    StructField("leagueAvg", DoubleType(), True)
])

struct_stats = StructType([
    StructField("gamesAbove900", struct_goal, True),
    StructField("goalDifferentialPer60", struct_goal, True),
    StructField("goalSupportAvg", struct_goal, True),
    StructField("goalsAgainstAvg", struct_goal, True),
    StructField("pointPctg", struct_goal, True)
])


edge_goalies_schema = StructType([
    StructField("stats", struct_stats, True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ./util_schema_drift_log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.read.table("bronze.fact_edge_stats_goalies")



new_fields, missing_fields, = check_schema_drift(
    bronze_df=df_bronze,
    raw_col="raw_json",
    expected_schema=edge_goalies_schema,
    source_entity="fact_edge_stats_goalies"
)

if missing_fields:
    print(f"Fields expected but not found in recent raw JSON: {missing_fields}")
# if new_fields:
#     print(f"New fields present in raw JSON but not in schema: {new_fields}")



df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), edge_goalies_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_edge_goalies = df_parsed.select(
    F.col("player_id"),
    F.col("season"),
    F.col("parsed.stats.gamesAbove900.value").alias("games_above_900"),
    F.col("parsed.stats.gamesAbove900.percentile").alias("games_above_900_percentile"),
    F.col("parsed.stats.gamesAbove900.leagueAvg").alias("games_above_900_leagueAvg"),
    F.col("parsed.stats.goalDifferentialPer60.value").alias("goal_differential_per_60"),
    F.col("parsed.stats.goalDifferentialPer60.percentile").alias("goal_differential_per_60_percentile"),
    F.col("parsed.stats.goalDifferentialPer60.leagueAvg").alias("goal_differential_per_60_leagueAvg"),
    F.col("parsed.stats.goalSupportAvg.value").alias("goal_support_avg"),
    F.col("parsed.stats.goalSupportAvg.percentile").alias("goal_support_avg_percentile"),
    F.col("parsed.stats.goalSupportAvg.leagueAvg").alias("goal_support_avg_leagueAvg"),
    F.col("parsed.stats.goalsAgainstAvg.value").alias("goals_against_avg"),
    F.col("parsed.stats.goalsAgainstAvg.percentile").alias("goals_against_avg_percentile"),
    F.col("parsed.stats.goalsAgainstAvg.leagueAvg").alias("goals_against_avg_leagueAvg"),
    F.col("parsed.stats.pointPctg.value").alias("point_pctg"),
    F.col("parsed.stats.pointPctg.percentile").alias("point_pctg_percentile"),
    F.col("parsed.stats.pointPctg.leagueAvg").alias("point_pctg_leagueAvg")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_edge_goalies.show(1, truncate=False, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_edge_goalies.write.format("delta").mode("overwrite").saveAsTable("silver.fact_edge_stats_goalies")

print(f"Table silver.fact_edge_stats_goalies saved with {df_fact_edge_goalies.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
