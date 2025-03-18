from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder.appName("Coalesced_Patient_CSV").getOrCreate()

#Validate patient data
#Input parquet folder
parquet_folder_patient = os.path.expanduser("~/Desktop/project_parquet/data_set7/raw_parquet/patients")

# Output CSV folder
csv_output_folder_patient = os.path.expanduser("~/Desktop/project_csv/data_set7/raw_csv/patients")

# Read the Parquet files
df = spark.read.parquet(parquet_folder_patient)

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
flat_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_output_folder_patient)

print("✅ Patient Parquet to CSV conversion complete.")

#Validate Encounter data

#Input parquet folder
parquet_folder_encounter = os.path.expanduser("~/Desktop/project_parquet/data_set7/raw_parquet/encounters")

# Output CSV folder
csv_output_folder_encounter = os.path.expanduser("~/Desktop/project_csv/data_set7/raw_csv/encounters")

# Read the Parquet files
df = spark.read.parquet(parquet_folder_encounter)

# Flatten schema by selecting only simple columns
flat_df = df.select(
    "id"
)

# Write as CSV
flat_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_output_folder_encounter)

print("✅ Encounter Parquet to CSV conversion complete.")

#Validate condition data

#Input parquet folder
parquet_folder_condition = os.path.expanduser("~/Desktop/project_parquet/data_set7/raw_parquet/conditions")

# Output CSV folder
csv_output_folder_condition = os.path.expanduser("~/Desktop/project_csv/data_set7/raw_csv/conditions")

# Read the Parquet files
df = spark.read.parquet(parquet_folder_condition)

# Flatten schema by selecting only simple columns
flat_df = df.select(
    "id"
)

# Write as CSV
flat_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_output_folder_condition)

print("✅ Condition Parquet to CSV conversion complete.")

