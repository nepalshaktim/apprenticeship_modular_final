from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder.appName("Coalesced_Patient_CSV").getOrCreate()

#Input parquet folder
parquet_folder = os.path.expanduser("~/Desktop/project_parquet/data_set7/raw_parquet/patients")

# Output CSV folder
csv_output_folder = os.path.expanduser("~/Desktop/project_csv/data_set7/raw_csv/patients")

# Read the Parquet files
df = spark.read.parquet(parquet_folder)

# Flatten schema by selecting only simple columns
flat_df = df.select(
    "id",
    "gender",
    "birthDate",
    "deceasedBoolean",
    "deceasedDateTime",
    "multipleBirthBoolean",
    "multipleBirthInteger",
    "resourceType",
    col("name")[0]["family"].alias("last_name"),
    col("name")[0]["given"][0].alias("first_name")
)

# Write as CSV
flat_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_output_folder)

print("✅ Parquet to CSV conversion complete.")