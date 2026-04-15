from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("USPS_Clean_Analysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(
    "gs://usps-pipeline-data/extract*.gz",
    header=True,
    inferSchema=True
)

rural_df = spark.read.csv(
    "gs://usps-pipeline-data/reference/forhp_rural_zipcode.csv",
    header=True,
    inferSchema=True
)

# Filter out -1 ZIP codes
df_clean = df.filter(col("orgn_zip_5") != "-1") \
             .filter(col("score").isNotNull()) \
             .filter(col("overall_moe") <= 0.05)

df_joined = df_clean.join(
    rural_df,
    df_clean.orgn_zip_5 == rural_df.ZIP_CODE,
    "left"
).withColumnRenamed("FORHP_Rural_approximation", "rural_urban")

# Rural vs Urban
print("=== RURAL vs URBAN (Clean Data) ===")
df_joined.groupBy("rural_urban") \
    .agg(
        count("*").alias("total_records"),
        avg("score").alias("avg_score"),
        avg("avg_days_to_delr").alias("avg_days"),
        avg("score_plus_1").alias("avg_score_plus_1")
    ).orderBy("rural_urban").show()

# Performance by mail type
print("=== PERFORMANCE BY MAIL TYPE ===")
df_joined.groupBy("prodt", "rural_urban") \
    .agg(
        count("*").alias("records"),
        avg("score").alias("avg_score")
    ).orderBy("prodt", "rural_urban").show(20, truncate=False)

# Bottom 20 worst performing districts
print("=== WORST 20 PERFORMING DISTRICTS (Rural) ===")
df_joined.filter(col("rural_urban") == "Yes") \
    .groupBy("orgn_dist_name") \
    .agg(
        count("*").alias("records"),
        avg("score").alias("avg_score"),
        avg("avg_days_to_delr").alias("avg_days")
    ).orderBy("avg_score") \
    .show(20, truncate=False)

# Save clean results
df_joined.write.mode("overwrite").parquet(
    "gs://usps-pipeline-data/processed/usps_joined"
)

print("\nDone!")
spark.stop()
