import os
import pandas as pd
import json


# Define file paths
json_file_path = os.path.expanduser("~/Desktop/project_json/data_set7/raw_json/conditions.json")
csv_file_path = os.path.expanduser("~/Desktop/project_csv/data_set7/cleaned_csv/conditions.csv")

# Load JSON file
with open(json_file_path, "r") as file:
    data = json.load(file)

# Flatten nested JSON structures
def flatten_nested_fields(record, parent_key=""):
    flattened = {}
    for key, value in record.items():
        new_key = f"{parent_key}.{key}" if parent_key else key
        if isinstance(value, dict):
            flattened.update(flatten_nested_fields(value, new_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flattened.update(flatten_nested_fields(item, f"{new_key}_{i}"))
                else:
                    flattened[f"{new_key}_{i}"] = item
        else:
            flattened[new_key] = value
    return flattened

flattened_records = [flatten_nested_fields(encounter) for encounter in data]
flattened_df = pd.DataFrame(flattened_records)

# Define columns to keep and rename them
columns_to_keep = {
    "id": "condition_id",
    "clinicalStatus.coding_0.display": "clinical_status",
    "verificationStatus.coding_0.display": "verification_status",
    "category_0.coding_0.display": "category",
    "severity.coding_0.display": "severity",
    "code.coding_0.display": "code",
    "bodySite_0.coding_0.code": "body_site_code",
    "bodySite_0.coding_0.display": "body_site",
    "subject.reference": "patient_id",
    "encounter.reference": "encounter_id",
    "onsetDateTime": "onset_date",
    "abatementDateTime": "abatement_date",
    "recordedDate": "recorded_date",
    "participant_0.function.coding_0.display": "participant_function",
    "participant_0.actor.reference": "participant_actor",
    "stage_0.summary.coding_0.code": "stage_summary_code",
    "stage_0.summary.coding_0.display": "stage_summary",
    "stage_0.type.coding_0.code": "stage_type_code",
    "stage_0.type.coding_0.display": "stage_type"
}

# Filter and rename columns
filtered_df = flattened_df[list(columns_to_keep.keys())].rename(columns=columns_to_keep)

# Ensure the output folder exists
os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

# Save to CSV
filtered_df.to_csv(csv_file_path, index=False)

print(f"CSV file has been saved at: {csv_file_path}")
