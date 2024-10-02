# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS nyc_airbnb;
# MAGIC USE nyc_airbnb;

# COMMAND ----------

import logging
# Provide logging
logging.basicConfig(filename='dbfs:/FileStore/logs/error_log.log', level=logging.INFO, format='%(asctime)s %(message)s')

# Ingest data into Bronze table from local files using schema inference
database = "nyc_airbnb" # database to store the data
standard = "dbfs:/FileStore/tables/raw/AB_NYC_2019_part1.csv" # standard file for schema ingestion
checkpoint = "dbfs:/FileStore/checkpoint"
bronze_table = "bronze_nyc_airbnb" # bronze layer

logging.info(f"Start stream into bronze layer")
schema = (spark.read
            .option("inferSchema", "true")
            .option("header", "true")
            .csv(standard)
            .limit(3)
            .schema)

df_bronze = (spark.readStream
             .format("csv")  # Use the csv format directly
             .option("header", "true")  #  infer headers from the first row
             .option("inferSchema", "true")  # Enable schema inference
             .schema(schema)
             .load("dbfs:/FileStore/tables/raw"))  # Path to local CSV files (on DBFS)

logging.info(f"write stream into bronze table")
# Write to the Bronze Delta Table
(df_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint+bronze_table)
    .outputMode("append")
    .toTable(f"{database}.{bronze_table}"))



# COMMAND ----------

silver_table = "silver_nyc_airbnb"
def transform_data(df_bronze):
    try:
        # Read from the Bronze table
        df_bronze = spark.readStream.format("delta").load(bronze_path)

        logging.info(f"Transform and clear out data from bronze table")
        # Apply transformations
        # Filter out rows where price is 0 or negative
        df_silver = df_bronze.filter(col("price") > 0)

        # Filter rows with null minimum_nights and availabity_365
        df_silver = df_silver(col("minimum_nights").minimum_nights.isNotNull() 
                            & col("availability_365").isNotNull())

        # Convert last_review to a valid date format and fill missing values with the earliest available date
        # Find earliest date
        earliest_date = df_bronze.select(min(to_date(col("last_review"), "yyyy-MM-dd")))

        # Convert last_review to date format and fill missing with the earliest date
        df_silver = df_silver.withColumn("last_review", to_date(col("last_review"), "yyyy-MM-dd")) \
                                    .na.fill({"last_review": earliest_date})

        # Handle missing values in reviews_per_month by setting them to 0
        df_silver = df_silver.na.fill({"reviews_per_month": 0})

        # Drop rows with missing latitude or longitude
        df_silver = df_silver.dropna(subset=["latitude", "longitude"])
        return df_silver

    except Exception as e:
        logging.error(f"Error processing dataframe: {e}")

df_silver = transform_data(df_bronze)
logging.info(f"Put cleared data into silver table")
# Write to silver table
(df_silver.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint + silver_table)
    .outputMode("append")
    .toTable(f"{database}.{silver_table}"))

# COMMAND ----------

# Time Travelling
def time_travel():
    # Get the Delta table history
    history_df = spark.sql(f"DESCRIBE HISTORY {database}.{silver_table}")

    # Fetch the second latest version
    previous_version = history_df.select("version").collect()[1][0] 

    # Load the previous version of the table
    silver_previous_df = spark.read.option("versionAsOf", previous_version).load(f"delta/{database}/{silver_table}")
    return silver_previous_df
