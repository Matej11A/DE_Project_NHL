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

# ### notebook for exploration and random code snipets

# CELL ********************

df_bronze = spark.read.table("bronze.dim_players")

sample_json_rdd = df_bronze.select("raw_json").rdd.map(lambda row: row["raw_json"])
inferred_df = spark.read.json(sample_json_rdd)
inferred_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

inferred_df.select("playerId", "firstName.default", "lastName.default", "position").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

inferred_df.limit(1).toPandas().to_dict(orient="records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
inferred_df.select("playerId", F.explode("seasonTotals").alias("season_row")).select("playerId", "season_row.*").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import functions as F

df_bronze = spark.read.table("bronze.dim_nhl_teams")

df_bronze.printSchema()

# df_bronze.select("conference.*").printSchema()
# df_bronze.select("division.*").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

skaters_rdd = spark.read.table("bronze.fact_edge_stats_skaters").select("raw_json").rdd.map(lambda row: row["raw_json"])
skaters_inferred = spark.read.json(skaters_rdd)
skaters_inferred.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

goalies_rdd = spark.read.table("bronze.fact_edge_stats_goalies").select("raw_json").rdd.map(lambda row: row["raw_json"])
goalies_inferred = spark.read.json(goalies_rdd)
goalies_inferred.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

games_rdd = spark.read.table("bronze.fact_games").select("raw_json").rdd.map(lambda row: row["raw_json"])
games_inferred = spark.read.json(games_rdd)
games_inferred.printSchema()

games_inferred.show(1, truncate=False, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

season_rdd = spark.read.table("bronze.dim_players").select("raw_json").rdd.map(lambda row: row["raw_json"])
season_inferred = spark.read.json(season_rdd)
season_inferred.printSchema()

season_inferred.show(1, truncate=False, vertical=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
season_inferred.select(F.explode("seasonTotals").alias("s")).select("s.timeOnIce", "s.avgToi").filter("s.timeOnIce IS NOT NULL").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_count_check = spark.table("silver.fact_games").count() == games_final.count()
null_rate_check = games_final.select(
    F.avg(F.col("winning_scorer_sk").isNull().cast("int")).alias("scorer_null_rate"),
    F.avg(F.col("winning_goalie_sk").isNull().cast("int")).alias("goalie_null_rate"),
    F.avg(F.col("home_team_sk").isNull().cast("int")).alias("home_team_null_rate"),
    F.avg(F.col("away_team_sk").isNull().cast("int")).alias("away_team_null_rate"),
)

print(f"Row count match: {row_count_check}")
null_rate_check.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_count_check = silver_career_totals.count() == df_career_totals.count()
null_rate_check = df_career_totals.select(
    F.avg(F.col("player_sk").isNull().cast("int")).alias("player_sk_null_rate")
)

print(f"Row count match: {row_count_check}")
null_rate_check.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
