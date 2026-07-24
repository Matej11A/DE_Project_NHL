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

start_date = "2000-10-01"
end_date = "2027-07-01"

date_df = spark.sql(f"""
    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date """)

dim_date_df = (
    date_df
    .withColumn("date_sk", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("calendar_year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
    .withColumn(
        "nhl_season",
        F.when(F.month("full_date") >= 10,
                (F.year("full_date") * 1000) + (F.year("full_date") + 1))
        .otherwise(((F.year("full_date") - 1 ) * 1000) + F.year("full_date"))
    )
    .select("date_sk", "full_date", "calendar_year", "month", "day", "day_of_week", "nhl_season")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date_df.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")
print(f"Table gold.dim_date saved with {dim_date_df.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
