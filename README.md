 
# Step 7 implementation depends on the chosen option. Task says we need to use Delta Lake’s constraint validation.
!!!BUT, this approach have caveats.
Delta Lake supports ACID guarantees which means either all the data will be appended or none of it will be appended.
WE'd better have to filter out the values that don’t satisfy the constraints before appending if we’d like to add this data to the Delta table.
this is already written in code

# Creating a silver table with constraints
`spark.sql("""
CREATE TABLE nyc_airbnb.silver_nyc_airbnb
(
    id INT,
    name STRING,
    host_id INT,
    host_name STRING,
    neighbourhood_group STRING,
    neighbourhood STRING,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    room_type STRING,
    price DECIMAL(10,2) NOT NULL,
    minimum_nights INT NOT NULL,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month DECIMAL(3,2),
    calculated_host_listings_count INT,
    availability_365 INT NOT NULL
)
USING delta;
""")`

# Transform data in bronze layer
`df_silver = df_bronze.filter(col("price") > 0)` # for brevity purposes code size has been reduced

# Insert data into a silver table
`(df_silver.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint + silver_table)
    .outputMode("append")
    .toTable(f"{database}.{silver_table}"))`
# IF our fields had invalid value DeltaInvariantViolationException would be raised and the data would not be appended to the table according to acid.
 
