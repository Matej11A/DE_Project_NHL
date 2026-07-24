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

silver_edge_skaters = spark.table("silver.fact_edge_stats_skaters")
gold_players = spark.table("gold.dim_players")
gold_date = spark.table("gold.dim_date")

skaters_with_date = silver_edge_skaters.withColumn(
    "season_rep_date",
    F.to_date(F.concat(F.substring(F.col("season"), 1, 4), F.lit("-10-01")))
)

players_dim = gold_players.alias("p")

skaters_with_player = (
    skaters_with_date.alias("e")
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
        "e.total_distance_skated_km", "e.total_distance_skated_mi", "e.total_distance_percentile",
        "e.max_game_distance_km", "e.max_game_distance_mi", "e.max_game_distance_percentile",
        "e.top_shot_speed_kmh", "e.top_shot_speed_mph", "e.top_shot_speed_percentile",
        "e.max_skating_speed_kmh", "e.max_skating_speed_mph", "e.max_skating_speed_percentile",
        "e.bursts_over_20mph", "e.bursts_over_20mph_percentile",
        "e.offensive_zone_pct", "e.offensive_zone_pct_league_avg", "e.offensive_zone_percentile",
        "e.offensive_zone_ev_pct", "e.offensive_zone_ev_pct_league_avg", "e.offensive_zone_ev_percentile",
        "e.neutral_zone_pct", "e.neutral_zone_pct_league_avg", "e.neutral_zone_percentile",
        "e.defensive_zone_pct", "e.defensive_zone_pct_league_avg", "e.defensive_zone_percentile",
        "e.total_distance_skated_leagueAvg_km", "e.total_distance_skated_leagueAvg_mi",
        "e.max_game_distance_leagueAvg_km", "e.max_game_distance_leagueAvg_mi",
        "e.top_shot_speed_leagueAvg_km", "e.top_shot_speed_leagueAvg_mi",
        "e.max_skating_speed_leagueAvg_km", "e.max_skating_speed_leagueAvg_mi",
        "e.burst_over_20mph_leagueAvg",
        F.col("p.player_sk").alias("player_sk"),
    )
)

date_dim = gold_date.alias("d")

fact_edge_stats_skaters = (
    skaters_with_player.alias("e2")
    .join(
        date_dim,
        on=F.col("e2.season_rep_date") == F.col("d.full_date"),
        how="left"
    )
    .select(
        "e2.player_id", "e2.season", "e2.player_sk",
        F.col("d.date_sk").alias("date_sk"),
        "e2.total_distance_skated_km", "e2.total_distance_skated_mi", "e2.total_distance_percentile",
        "e2.max_game_distance_km", "e2.max_game_distance_mi", "e2.max_game_distance_percentile",
        "e2.top_shot_speed_kmh", "e2.top_shot_speed_mph", "e2.top_shot_speed_percentile",
        "e2.max_skating_speed_kmh", "e2.max_skating_speed_mph", "e2.max_skating_speed_percentile",
        "e2.bursts_over_20mph", "e2.bursts_over_20mph_percentile",
        "e2.offensive_zone_pct", "e2.offensive_zone_pct_league_avg", "e2.offensive_zone_percentile",
        "e2.offensive_zone_ev_pct", "e2.offensive_zone_ev_pct_league_avg", "e2.offensive_zone_ev_percentile",
        "e2.neutral_zone_pct", "e2.neutral_zone_pct_league_avg", "e2.neutral_zone_percentile",
        "e2.defensive_zone_pct", "e2.defensive_zone_pct_league_avg", "e2.defensive_zone_percentile",
        "e2.total_distance_skated_leagueAvg_km", "e2.total_distance_skated_leagueAvg_mi",
        "e2.max_game_distance_leagueAvg_km", "e2.max_game_distance_leagueAvg_mi",
        "e2.top_shot_speed_leagueAvg_km", "e2.top_shot_speed_leagueAvg_mi",
        "e2.max_skating_speed_leagueAvg_km", "e2.max_skating_speed_leagueAvg_mi",
        "e2.burst_over_20mph_leagueAvg",
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_edge_stats_skaters.write.format("delta").mode("overwrite").saveAsTable("gold.fact_edge_stats_skaters")
print(f"Table gold.fact_edge_stats_skaters saved with {fact_edge_stats_skaters.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_count_check = silver_edge_skaters.count() == fact_edge_stats_skaters.count()
null_rate_check = fact_edge_stats_skaters.select(
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
