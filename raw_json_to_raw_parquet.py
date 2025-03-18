from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, to_date
import os

spark = SparkSession.builder.appName("PartitionedPatientParquet").getOrCreate()

#Patient data json to parquet conversion

json_path_patient = os.path.expanduser("~/Desktop/project_json/data_set7/raw_json/patients.json")
parquet_folder_patient = os.path.expanduser("~/Desktop/project_parquet/data_set7/raw_parquet/patients")

# Read JSON with multiline mode
df = spark.read.option("multiline", "true").json(json_path_patient)

# Check schema
#df.printSchema()

#Extract birth_year from birthDate column for each patient for meaningful partition
df = df.withColumn("birth_year", year(to_date("birthDate")))

# Write Parquet partitioned by birth_year
df.repartition("birth_year").write.mode("overwrite").partitionBy("birth_year").parquet(parquet_folder_patient)

print("✅✅✅✅ Patient data Json to Parquet conversion complete.")

#Encounter data json to parquet conversion

json_path_encounter = os.path.expanduser("~/Desktop/project_json/data_set7/raw_json/encounters.json")
parquet_folder_encounter = os.path.expanduser("~/Desktop/project_parquet/data_set7/raw_parquet/encounters")

# Read JSON with multiline mode
df = spark.read.option("multiline", "true").json(json_path_encounter)

# Check schema
#df.printSchema()

#Extract encounter_start_month from actual_encounter_start column for each patient for meaningful partition
df = df.withColumn("encounter_start_month", month(to_date(col("actualPeriod.start")))) \
    .withColumn("patient_id", col("subject.reference"))

# Write Parquet partitioned by encounter_start_month and patient_id
df.repartition("encounter_start_month", "patient_id") \
    .write.mode("overwrite") \
    .partitionBy("encounter_start_month","patient_id") \
    .parquet(parquet_folder_encounter)

print("✅✅✅✅ Encounter data Json to Parquet conversion complete.")

#Condition data json to parquet conversion

json_path_condition = os.path.expanduser("~/Desktop/project_json/data_set7/raw_json/conditions.json")
parquet_folder_condition = os.path.expanduser("~/Desktop/project_parquet/data_set7/raw_parquet/conditions")

# Read JSON with multiline mode
df = spark.read.option("multiline", "true").json(json_path_condition)

# Check schema
#df.printSchema()

#Extract encounter_start_month from actual_encounter_start column for each patient for meaningful partition
df = df.withColumn("recorded_date_year", year(to_date(col("recordedDate")))) \
    .withColumn("recorded_date_month", month(to_date(col("recordedDate")))) \
    .withColumn("clinical_status", col("clinicalStatus.coding")[0]["display"])

# Write Parquet partitioned by condition recorded_date_year, recorded_date_month and clinical_status
df.repartition("recorded_date_year", "recorded_date_month", "clinical_status") \
    .write.mode("overwrite") \
    .partitionBy("recorded_date_year", "recorded_date_month", "clinical_status") \
    .parquet(parquet_folder_condition)

print("✅✅✅✅ Condition data Json to Parquet conversion complete.")