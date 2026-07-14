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

# # notebook to pull raw edge stats data for ANA skaters (forwards and defensemen)

# CELL ********************

%pip install nhl-api-py

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time
from nhlpy import NHLClient
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

client = NHLClient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SEASON = "20242025"
TEAM_ABBR = "ANA"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"1. Fetching {SEASON} roster for {TEAM_ABBR}...")
roster = client.teams.team_roster(team_abbr=TEAM_ABBR, season=SEASON)

skaters = roster.get("forwards", []) + roster.get("defensemen", [])
print(f" Found {len(skaters)} skaters on the roster.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_edge_rows = []

print("\n2. Pulling live EDGE profiles...")
for idx, skater in enumerate(skaters, start=1):
    player_id = skater.get("id")
    first_name = skater.get("firstName", {}).get("default", "")
    last_name = skater.get("lastName", {}).get("default", "")
    player_name = f"{first_name} {last_name}".strip()

    if not player_id:
        continue

    print(f"  [{idx}/{len(skaters)}] Extracting: {player_name}")

    try:
        raw_edge_data = client.edge.skater_detail(player_id=player_id, season=SEASON)

        if raw_edge_data:
            raw_edge_rows.append({
                "player_id": player_id,
                "player_name": player_name,
                "position_code": skater.get("positionCode"),
                "season": SEASON,
                "raw_json": json.dumps(raw_edge_data)
            })

    except Exception as player_err:
        print(f"    Could not fetch data for {player_name}: {player_err}")

    time.sleep(0.2)

print(f" \nDone - {len(raw_edge_rows)} skater EDGE profiles fetched")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_schema = StructType([
    StructField("player_id", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("position_code", StringType(), True),
    StructField("season", StringType(), True),
    StructField("raw_json", StringType(), True),
])

df_bronze = spark.createDataFrame(raw_edge_rows, schema=bronze_schema)
df_bronze = df_bronze.withColumn("_ingested_at", F.current_timestamp())

df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze.fact_edge_stats_skaters")

print(f"Saved bronze.fact_edge_stats_skaters table with {df_bronze.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
