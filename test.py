from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("PartitionedPatientParquet").getOrCreate()

json_path = os.path.expanduser("~/Desktop/project_json/data_set7/raw_json/patients.json")
parquet_folder = os.path.expanduser("~/Desktop/project_parquet/data_set7/raw_parquet/patients")

# Read JSON with multiline mode
df = spark.read.option("multiline", "true").json(json_path)

# Check schema
df.printSchema()

# Write Parquet partitioned by gender
df.write.mode("overwrite").partitionBy("birthDate").parquet(parquet_folder)

print("✅ Json to Parquet conversion complete.")

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
    "resourceType"
)

# Write as CSV
flat_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_output_folder)

print("✅ Parquet to CSV conversion is complete.")
