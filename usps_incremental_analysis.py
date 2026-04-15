from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.cloud import storage
import json
from datetime import datetime

spark = SparkSession.builder.appName("USPS_Incremental").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BUCKET = "usps-pipeline-data"
PROCESSED_LOG = "processed/processed_files.json"

client = storage.Client()
bucket = client.bucket(BUCKET)

# Step 1 - Check which files already processed
try:
    blob = bucket.blob(PROCESSED_LOG)
    processed_files = json.loads(blob.download_as_text())
    print(f"Already processed: {len(processed_files)} files")
except:
    processed_files = []
    print("First run - no processed files yet")

# Step 2 - Find new files in bucket
all_files = [
    b.name for b in bucket.list_blobs()
    if b.name.startswith('extract') and b.name.endswith('.gz')
]

new_files = [f for f in all_files if f not in processed_files]
print(f"New files to process: {len(new_files)}")

if len(new_files) == 0:
    print("No new files — dashboard is already up to date!")
    spark.stop()
    exit()

# Step 3 - Process only NEW files
print("Reading new files...")
new_paths = [f"gs://{BUCKET}/{f}" for f in new_files]
df_new = spark.read.csv(new_paths, header=True, inferSchema=True)
print(f"New rows: {df_new.count():,}")

df_new = df_new.filter(col("orgn_zip_5") != "-1") \
               .filter(col("score").isNotNull()) \
               .filter(col("overall_moe") <= 0.05)

rural_df = spark.read.csv(
    f"gs://{BUCKET}/reference/forhp_rural_zipcode.csv",
    header=True, inferSchema=True
)

df_joined = df_new.join(
    rural_df,
    df_new.orgn_zip_5 == rural_df.ZIP_CODE,
    "left"
).withColumnRenamed("FORHP_Rural_approximation", "rural_urban")

# Step 4 - Calculate new summaries
new_rural_urban = df_joined.groupBy("rural_urban").agg(
    count("*").alias("total_records"),
    avg("score").alias("avg_score"),
    avg("avg_days_to_delr").alias("avg_days"),
    avg("score_plus_1").alias("avg_score_plus_1")
)

new_district = df_joined.groupBy("orgn_dist_name", "rural_urban").agg(
    count("*").alias("records"),
    avg("score").alias("avg_score"),
    avg("avg_days_to_delr").alias("avg_days")
)

new_mailtype = df_joined.groupBy("prodt", "rural_urban").agg(
    count("*").alias("records"),
    avg("score").alias("avg_score")
)

# Step 5 - Read old results into memory FIRST before overwriting
try:
    old_rural_urban = spark.read.csv(
        f"gs://{BUCKET}/results/rural_urban_summary/",
        header=True, inferSchema=True
    ).cache()  # cache in memory!

    old_district = spark.read.csv(
        f"gs://{BUCKET}/results/district_summary/",
        header=True, inferSchema=True
    ).cache()

    old_mailtype = spark.read.csv(
        f"gs://{BUCKET}/results/mailtype_summary/",
        header=True, inferSchema=True
    ).cache()

    # Force cache to load into memory NOW
    old_rural_urban.count()
    old_district.count()
    old_mailtype.count()
    print("Old results cached in memory!")

    # Now merge
    combined_rural_urban = old_rural_urban.union(new_rural_urban) \
        .groupBy("rural_urban").agg(
            sum("total_records").alias("total_records"),
            avg("avg_score").alias("avg_score"),
            avg("avg_days").alias("avg_days"),
            avg("avg_score_plus_1").alias("avg_score_plus_1")
        )

    combined_district = old_district.union(new_district) \
        .groupBy("orgn_dist_name", "rural_urban").agg(
            sum("records").alias("records"),
            avg("avg_score").alias("avg_score"),
            avg("avg_days").alias("avg_days")
        )

    combined_mailtype = old_mailtype.union(new_mailtype) \
        .groupBy("prodt", "rural_urban").agg(
            sum("records").alias("records"),
            avg("avg_score").alias("avg_score")
        )

except Exception as e:
    print(f"No existing results or error: {e}")
    combined_rural_urban = new_rural_urban
    combined_district = new_district
    combined_mailtype = new_mailtype

# Step 6 - Save results
print("Saving updated results...")
combined_rural_urban.write.mode("overwrite").csv(
    f"gs://{BUCKET}/results/rural_urban_summary", header=True)
combined_district.write.mode("overwrite").csv(
    f"gs://{BUCKET}/results/district_summary", header=True)
combined_mailtype.write.mode("overwrite").csv(
    f"gs://{BUCKET}/results/mailtype_summary", header=True)

# Step 7 - Update log
updated_processed = processed_files + new_files
blob = bucket.blob(PROCESSED_LOG)
blob.upload_from_string(json.dumps(updated_processed))
print(f"Log updated: {len(updated_processed)} total files processed")

print(f"\nDone! {len(new_files)} new files processed at {datetime.now()}")
spark.stop()
