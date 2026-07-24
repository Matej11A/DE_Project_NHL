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

silver_edge_skaters_summary = spark.table("silver.fact_edge_stats_skaters_summary")
gold_players = spark.table("gold.dim_players")
gold_date = spark.table("gold.dim_date")

summary_with_date = silver_edge_skaters_summary.withColumn(
    "season_rep_date",
    F.to_date(F.concat(F.substring(F.col("season"), 1, 4), F.lit("-10-01")))
)

players_dim = gold_players.alias("p")

summary_with_player = (
    summary_with_date.alias("e")
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
        "e.player_id", "e.season", "e.season_rep_date", "e.location_code",
        "e.goals", "e.goals_league_avg", "e.goals_percentile",
        "e.shooting_pctg", "e.shooting_pctg_league_avg", "e.shooting_pctg_percentile",
        "e.shots", "e.shots_league_avg", "e.shots_percentile",
        F.col("p.player_sk").alias("player_sk"),
    )
)

date_dim = gold_date.alias("d")

fact_edge_stats_skaters_summary = (
    summary_with_player.alias("e2")
    .join(
        date_dim,
        on=F.col("e2.season_rep_date") == F.col("d.full_date"),
        how="left"
    )
    .select(
        "e2.player_id", "e2.season", "e2.location_code", "e2.player_sk",
        F.col("d.date_sk").alias("date_sk"),
        "e2.goals", "e2.goals_league_avg", "e2.goals_percentile",
        "e2.shooting_pctg", "e2.shooting_pctg_league_avg", "e2.shooting_pctg_percentile",
        "e2.shots", "e2.shots_league_avg", "e2.shots_percentile",
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_edge_stats_skaters_summary.write.format("delta").mode("overwrite").saveAsTable("gold.fact_edge_stats_skaters_summary")
print(f"Table gold.fact_edge_stats_skaters_summary saved with {fact_edge_stats_skaters_summary.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Verification ---
row_count_check = silver_edge_skaters_summary.count() == fact_edge_stats_skaters_summary.count()
null_rate_check = fact_edge_stats_skaters_summary.select(
    F.avg(F.col("player_sk").isNull().cast("int")).alias("player_sk_null_rate"),
    F.avg(F.col("date_sk").isNull().cast("int")).alias("date_sk_null_rate"),
)

print(f"Row count match: {row_count_check}")
null_rate_check.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
