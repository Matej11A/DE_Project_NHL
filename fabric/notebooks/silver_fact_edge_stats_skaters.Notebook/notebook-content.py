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

# # defining scalar values from EDGE stats for skaters

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# reusable schema definiton

league_avg_metric_struct = StructType([
    StructField("imperial", DoubleType(), True),
    StructField("metric", DoubleType(), True)
])

metric_value_struct = StructType([
    StructField("imperial", DoubleType(), True),
    StructField("metric", DoubleType(), True),
    StructField("percentile", DoubleType(), True),
    StructField("leagueAvg", league_avg_metric_struct, True)
])

bursts_struct = StructType([
    StructField("value", DoubleType(), True),
    StructField("percentile", DoubleType(), True),
    StructField("leagueAvg", StructType([
        StructField("value", DoubleType(), True)
    ]), True)
])

zone_time_struct = StructType([
    StructField("defensiveZonePctg", DoubleType(), True),
    StructField("defensiveZoneLeagueAvg", DoubleType(), True),
    StructField("defensiveZonePercentile", DoubleType(), True),
    StructField("neutralZonePctg", DoubleType(), True),
    StructField("neutralZoneLeagueAvg", DoubleType(), True),
    StructField("neutralZonePercentile", DoubleType(), True),
    StructField("offensiveZonePctg", DoubleType(), True),
    StructField("offensiveZoneLeagueAvg", DoubleType(), True),
    StructField("offensiveZonePercentile", DoubleType(), True),
    StructField("offensiveZoneEvPctg", DoubleType(), True),
    StructField("offensiveZoneEvLeagueAvg", DoubleType(), True),
    StructField("offensiveZoneEvPercentile", DoubleType(), True),
])

edge_skater_schema = StructType([
    StructField("distanceMaxGame", metric_value_struct, True),
    StructField("skatingSpeed", StructType([
        StructField("burstsOver20", bursts_struct, True),
        StructField("speedMax", metric_value_struct, True)
    ]), True),
    StructField("topShotSpeed", metric_value_struct, True),
    StructField("totalDistanceSkated", metric_value_struct, True),
    StructField("zoneTimeDetails", zone_time_struct, True)
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

df_bronze = spark.read.table("bronze.fact_edge_stats_skaters")


new_fields, missing_fields, = check_schema_drift(
    bronze_df=df_bronze,
    raw_col="raw_json",
    expected_schema=edge_skater_schema,
    source_entity="fact_edge_stats_skaters"
)

if missing_fields:
    print(f"Fields expected but not found in recent raw JSON: {missing_fields}")
# if new_fields:
#     print(f"New fields present in raw JSON but not in schema: {new_fields}")



df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), edge_skater_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_edge_stats_skaters = df_parsed.select(
    F.col("player_id"),
    F.col("season"),
    F.col("parsed.totalDistanceSkated.metric").alias("total_distance_skated_km"),
    F.col("parsed.totalDistanceSkated.imperial").alias("total_distance_skated_mi"),
    F.col("parsed.totalDistanceSkated.percentile").alias("total_distance_percentile"),
    F.col("parsed.totalDistanceSkated.leagueAvg.metric").alias("total_distance_skated_leagueAvg_km"),
    F.col("parsed.totalDistanceSkated.leagueAvg.imperial").alias("total_distance_skated_leagueAvg_mi"),
    F.col("parsed.distanceMaxGame.metric").alias("max_game_distance_km"),
    F.col("parsed.distanceMaxGame.imperial").alias("max_game_distance_mi"),
    F.col("parsed.distanceMaxGame.percentile").alias("max_game_distance_percentile"),
    F.col("parsed.distanceMaxGame.leagueAvg.metric").alias("max_game_distance_leagueAvg_km"),
    F.col("parsed.distanceMaxGame.leagueAvg.imperial").alias("max_game_distance_leagueAvg_mi"),
    F.col("parsed.topShotSpeed.metric").alias("top_shot_speed_kmh"),
    F.col("parsed.topShotSpeed.imperial").alias("top_shot_speed_mph"),
    F.col("parsed.topShotSpeed.percentile").alias("top_shot_speed_percentile"),
    F.col("parsed.topShotSpeed.leagueAvg.metric").alias("top_shot_speed_leagueAvg_km"),
    F.col("parsed.topShotSpeed.leagueAvg.imperial").alias("top_shot_speed_leagueAvg_mi"),
    F.col("parsed.skatingSpeed.speedMax.metric").alias("max_skating_speed_kmh"),
    F.col("parsed.skatingSpeed.speedMax.imperial").alias("max_skating_speed_mph"),
    F.col("parsed.skatingSpeed.speedMax.percentile").alias("max_skating_speed_percentile"),
    F.col("parsed.skatingSpeed.speedMax.leagueAvg.metric").alias("max_skating_speed_leagueAvg_km"),
    F.col("parsed.skatingSpeed.speedMax.leagueAvg.imperial").alias("max_skating_speedd_leagueAvg_mi"),
    F.col("parsed.skatingSpeed.burstsOver20.value").alias("bursts_over_20mph"),
    F.col("parsed.skatingSpeed.burstsOver20.percentile").alias("bursts_over_20mph_percentile"),
    F.col("parsed.skatingSpeed.burstsOver20.leagueAvg.value").alias("burst_over_20mph_leagueAvg"),
    F.col("parsed.zoneTimeDetails.offensiveZonePctg").alias("offensive_zone_pct"),
    F.col("parsed.zoneTimeDetails.offensiveZoneLeagueAvg").alias("offensive_zone_pct_league_avg"),
    F.col("parsed.zoneTimeDetails.offensiveZonePercentile").alias("offensive_zone_percentile"),
    F.col("parsed.zoneTimeDetails.offensiveZoneEvPctg").alias("offensive_zone_ev_pct"),
    F.col("parsed.zoneTimeDetails.offensiveZoneEvLeagueAvg").alias("offensive_zone_ev_pct_league_avg"),
    F.col("parsed.zoneTimeDetails.offensiveZoneEvPercentile").alias("offensive_zone_ev_percentile"),
    F.col("parsed.zoneTimeDetails.neutralZonePctg").alias("neutral_zone_pct"),
    F.col("parsed.zoneTimeDetails.neutralZoneLeagueAvg").alias("neutral_zone_pct_league_avg"),
    F.col("parsed.zoneTimeDetails.neutralZonePercentile").alias("neutral_zone_percentile"),
    F.col("parsed.zoneTimeDetails.defensiveZonePctg").alias("defensive_zone_pct"),
    F.col("parsed.zoneTimeDetails.defensiveZoneLeagueAvg").alias("defensive_zone_pct_league_avg"),
    F.col("parsed.zoneTimeDetails.defensiveZonePercentile").alias("defensive_zone_percentile"),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_edge_stats_skaters.show(1, truncate=False, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_edge_stats_skaters.write.format("delta").mode("overwrite").option("mergeSchema", True).saveAsTable("silver.fact_edge_stats_skaters")

print(f"Saved silver.fact_edge_stats_skaters with {df_fact_edge_stats_skaters.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
