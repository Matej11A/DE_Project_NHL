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

silver_edge_goalies_detail = spark.table("silver.fact_edge_stats_goalies_detail")
gold_players = spark.table("gold.dim_players")
gold_date = spark.table("gold.dim_date")


detail_with_date = silver_edge_goalies_detail.withColumn(
    "season_rep_date",
    F.to_date(F.concat(F.substring(F.col("season"), 1, 4), F.lit("-10-01")))
)

players_dim = gold_players.alias("p")

detail_with_player = (
    detail_with_date.alias("e")
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
        "e.player_id", "e.season", "e.season_rep_date", "e.area",
        "e.save_pctg", "e.save_pctg_percentile",
        "e.saves", "e.saves_percentile",
        F.col("p.player_sk").alias("player_sk"),
    )
)

date_dim = gold_date.alias("d")

fact_edge_stats_goalies_detail = (
    detail_with_player.alias("e2")
    .join(
        date_dim,
        on=F.col("e2.season_rep_date") == F.col("d.full_date"),
        how="left"
    )
    .select(
        "e2.player_id", "e2.season", "e2.area", "e2.player_sk",
        F.col("d.date_sk").alias("date_sk"),
        "e2.save_pctg", "e2.save_pctg_percentile",
        "e2.saves", "e2.saves_percentile",
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_edge_stats_goalies_detail.write.format("delta").mode("overwrite").saveAsTable("gold.fact_edge_stats_goalies_detail")
print(f"Table gold.fact_edge_stats_goalies_detail saved with {fact_edge_stats_goalies_detail.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Verification ---
row_count_check = silver_edge_goalies_detail.count() == fact_edge_stats_goalies_detail.count()
null_rate_check = fact_edge_stats_goalies_detail.select(
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
