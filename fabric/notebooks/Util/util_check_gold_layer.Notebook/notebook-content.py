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

# Each entry: (gold_table, source_silver_table, [surrogate_key_columns_to_check])
fact_table_checks = [
    ("gold.fact_games", "silver.fact_games", ["winning_scorer_sk", "winning_goalie_sk", "home_team_sk", "away_team_sk"]),
    ("gold.fact_player_career_totals", "silver.fact_player_career_totals", ["player_sk"]),
    ("gold.fact_edge_stats_goalies", "silver.fact_edge_stats_goalies", ["player_sk", "date_sk"]),
    ("gold.fact_edge_stats_goalies_summary", "silver.fact_edge_stats_goalies_summary", ["player_sk", "date_sk"]),
    ("gold.fact_edge_stats_goalies_detail", "silver.fact_edge_stats_goalies_detail", ["player_sk", "date_sk"]),
    ("gold.fact_edge_stats_skaters", "silver.fact_edge_stats_skaters", ["player_sk", "date_sk"]),
    ("gold.fact_edge_stats_skaters_summary", "silver.fact_edge_stats_skaters_summary", ["player_sk", "date_sk"]),
    ("gold.fact_edge_stats_skaters_detail", "silver.fact_edge_stats_skaters_detail", ["player_sk", "date_sk"]),
]

results = []

for gold_table, silver_table, sk_columns in fact_table_checks:
    gold_df = spark.table(gold_table)
    silver_df = spark.table(silver_table)

    gold_count = gold_df.count()
    silver_count = silver_df.count()
    row_count_match = gold_count == silver_count

    null_rate_exprs = [
        F.avg(F.col(sk).isNull().cast("int")).alias(f"{sk}_null_rate")
        for sk in sk_columns
    ]
    null_rates = gold_df.select(*null_rate_exprs).collect()[0].asDict()

    results.append({
        "table": gold_table,
        "row_count_match": row_count_match,
        "gold_count": gold_count,
        "silver_count": silver_count,
        **null_rates,
    })

summary_df = spark.createDataFrame(results)
summary_df.show(truncate=False)

problems = summary_df.filter(
    (F.col("row_count_match") == False) |
    F.exists(
        F.array(*[F.col(c) for c in summary_df.columns if c.endswith("_null_rate")]),
        lambda x: x > 0.0
    )
)

if problems.count() > 0:
    print("Tables with row-count mismatches or non-zero SK null rates:")
    problems.show(truncate=False)
else:
    print("All tables: row counts match, all surrogate key null rates are 0%.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
