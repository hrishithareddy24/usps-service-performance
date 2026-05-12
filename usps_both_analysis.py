from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("USPS_Both_Analysis") \
    .config("spark.sql.files.ignoreCorruptFiles", "true") \
    .config("spark.sql.files.ignoreMissingFiles", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BUCKET = "usps-pipeline-data"

print("Step 1: Reading all USPS files...")
df = spark.read.option("ignoreCorruptFiles", "true") \
    .csv(f"gs://{BUCKET}/extract*.gz", header=True, inferSchema=True)

print("Step 2: Cleaning data...")
df = df.filter(col("score").isNotNull()) \
       .filter(col("overall_moe") <= 0.05)

print("Step 3: Loading rural/urban classification...")
rural_df = spark.read.csv(
    f"gs://{BUCKET}/reference/forhp_rural_zipcode.csv",
    header=True, inferSchema=True
)

rural_origin = rural_df.withColumnRenamed("ZIP_CODE", "orgn_zip_5") \
    .withColumnRenamed("FORHP_Rural_approximation", "origin_rural")

rural_dest = rural_df.withColumnRenamed("ZIP_CODE", "destn_zip_5") \
    .withColumnRenamed("FORHP_Rural_approximation", "dest_rural")

print("Step 4: Joining origin and destination...")
df_joined = df.join(rural_origin, "orgn_zip_5", "left") \
              .join(rural_dest, "destn_zip_5", "left")

df_joined = df_joined.withColumn(
    "origin_rural_label",
    when(col("origin_rural") == "Yes", "Rural")
    .when(col("origin_rural") == "No", "Urban")
    .otherwise("National")
).withColumn(
    "dest_rural_label",
    when(col("dest_rural") == "Yes", "Rural")
    .when(col("dest_rural") == "No", "Urban")
    .otherwise("National")
)

print("Step 5: Analyzing by origin (sending)...")
origin_summary = df_joined.filter(col("origin_rural_label") != "National") \
    .groupBy("origin_rural_label") \
    .agg(
        count("*").alias("total_records"),
        avg("score").alias("avg_score"),
        avg("avg_days_to_delr").alias("avg_days"),
        avg("score_plus_1").alias("avg_score_plus_1")
    )

print("=== SENDING PERFORMANCE (Origin) ===")
origin_summary.show()

print("Step 6: Analyzing by destination (receiving)...")
dest_summary = df_joined.filter(col("dest_rural_label") != "National") \
    .groupBy("dest_rural_label") \
    .agg(
        count("*").alias("total_records"),
        avg("score").alias("avg_score"),
        avg("avg_days_to_delr").alias("avg_days"),
        avg("score_plus_1").alias("avg_score_plus_1")
    )

print("=== RECEIVING PERFORMANCE (Destination) ===")
dest_summary.show()

origin_summary.write.mode("overwrite").csv(
    f"gs://{BUCKET}/results/origin_summary", header=True)
dest_summary.write.mode("overwrite").csv(
    f"gs://{BUCKET}/results/dest_summary", header=True)

print("Done!")
spark.stop()
