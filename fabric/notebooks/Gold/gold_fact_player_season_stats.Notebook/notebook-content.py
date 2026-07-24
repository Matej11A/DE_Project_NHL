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

silver_season_stats = spark.table("silver.fact_season_stats")
gold_players = spark.table("gold.dim_players")
gold_date = spark.table("gold.dim_date")

silver_season_stats_nhl = silver_season_stats.filter(F.col("league_abbrev") == "NHL")

season_stats_with_date = (
    silver_season_stats_nhl
    .withColumn(
        "season_rep_date",
        F.to_date(F.concat(F.substring(F.col("season"), 1, 4), F.lit("-10-01")))
    )
)

players_dim = gold_players.alias("p")

season_stats_with_player = (
    season_stats_with_date.alias("e")
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
        "e.player_id", "e.season", "e.season_rep_date", "e.sequence",
        "e.game_type_id", "e.league_abbrev", "e.team_name",
        "e.games_played", "e.games_started", "e.goals", "e.assists", "e.points",
        "e.plus_minus", "e.pim", "e.shots", "e.shooting_pctg", "e.ot_goals",
        "e.avg_toi", "e.time_on_ice", "e.game_winning_goals",
        "e.power_play_goals", "e.power_play_points",
        "e.shorthanded_goals", "e.shorthanded_points", "e.faceoff_winning_pctg",
        "e.wins", "e.losses", "e.ties", "e.ot_losses",
        "e.goals_against", "e.shots_against", "e.save_pctg", "e.shutouts",
        F.col("p.player_sk").alias("player_sk"),
    )
)

date_dim = gold_date.alias("d")

fact_season_stats = (
    season_stats_with_player.alias("e2")
    .join(
        date_dim,
        on=F.col("e2.season_rep_date") == F.col("d.full_date"),
        how="left"
    )
    .select(
        "e2.player_id", "e2.season", "e2.sequence", "e2.game_type_id",
        "e2.league_abbrev", "e2.team_name", "e2.player_sk",
        F.col("d.date_sk").alias("date_sk"),
        "e2.games_played", "e2.games_started", "e2.goals", "e2.assists", "e2.points",
        "e2.plus_minus", "e2.pim", "e2.shots", "e2.shooting_pctg", "e2.ot_goals",
        "e2.avg_toi", "e2.time_on_ice", "e2.game_winning_goals",
        "e2.power_play_goals", "e2.power_play_points",
        "e2.shorthanded_goals", "e2.shorthanded_points", "e2.faceoff_winning_pctg",
        "e2.wins", "e2.losses", "e2.ties", "e2.ot_losses",
        "e2.goals_against", "e2.shots_against", "e2.save_pctg", "e2.shutouts",
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_season_stats.write.format("delta").mode("overwrite").saveAsTable("gold.fact_season_stats")
print(f"Table gold.fact_season_stats saved with {fact_season_stats.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




# --- Verification ---
row_count_check = silver_season_stats_nhl.count() == fact_season_stats.count()
null_rate_check = fact_season_stats.select(
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
