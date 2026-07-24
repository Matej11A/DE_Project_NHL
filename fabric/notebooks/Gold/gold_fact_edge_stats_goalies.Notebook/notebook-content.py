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

silver_edge_goalies = spark.table("silver.fact_edge_stats_goalies")
gold_players = spark.table("gold.dim_players")
gold_date = spark.table("gold.dim_date")

edge_goalies_with_date = silver_edge_goalies.withColumn(
    "season_rep_date",
    F.to_date(F.concat(F.substring(F.col("season"), 1, 4), F.lit("-10-01")))
)

players_dim = gold_players.alias("p")

edge_goalies_with_player = (
    edge_goalies_with_date.alias("e")
    .join(
        players_dim,
        on=(
            (F.col("e.player_id") == F.col("p.player_id")) &
            (F.col("e.season_rep_date") >= F.col("p.effective_start_date")) &
            (F.col("e.season_rep_date") < F.col("p.effective_end_date"))
        ),
        how="left"
    )
    .select(
        "e.player_id", "e.season", "e.season_rep_date",
        "e.games_above_900", "e.games_above_900_percentile", "e.games_above_900_leagueAvg",
        "e.goal_differential_per_60", "e.goal_differential_per_60_percentile", "e.goal_differential_per_60_leagueAvg",
        "e.goal_support_avg", "e.goal_support_avg_percentile", "e.goal_support_avg_leagueAvg",
        "e.goals_against_avg", "e.goals_against_avg_percentile", "e.goals_against_avg_leagueAvg",
        "e.point_pctg", "e.point_pctg_percentile", "e.point_pctg_leagueAvg",
        F.col("p.player_sk").alias("player_sk"),
    )
)

date_dim = gold_date.alias("d")

fact_edge_stats_goalies = (
    edge_goalies_with_player.alias("e2")
    .join(
        date_dim,
        on=F.col("e2.season_rep_date") == F.col("d.full_date"),
        how="left"
    )
    .select(
        "e2.player_id", "e2.season", "e2.player_sk",
        F.col("d.date_sk").alias("date_sk"),
        "e2.games_above_900", "e2.games_above_900_percentile", "e2.games_above_900_leagueAvg",
        "e2.goal_differential_per_60", "e2.goal_differential_per_60_percentile", "e2.goal_differential_per_60_leagueAvg",
        "e2.goal_support_avg", "e2.goal_support_avg_percentile", "e2.goal_support_avg_leagueAvg",
        "e2.goals_against_avg", "e2.goals_against_avg_percentile", "e2.goals_against_avg_leagueAvg",
        "e2.point_pctg", "e2.point_pctg_percentile", "e2.point_pctg_leagueAvg",
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_edge_stats_goalies.write.format("delta").mode("overwrite").saveAsTable("gold.fact_edge_stats_goalies")
print(f"Table gold.fact_edge_stats_goalies saved with {fact_edge_stats_goalies.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
