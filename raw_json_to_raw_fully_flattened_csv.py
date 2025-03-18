import os
import pandas as pd
import json

# Define file paths
json_file_path = os.path.expanduser("~/Desktop/project_json/data_set6/raw_json/patients.json")
csv_file_path = os.path.expanduser("~/Desktop/project_csv/data_set6/raw_csv/patients.csv")

# Load JSON file
with open(json_file_path, "r") as file:
    data = json.load(file)

# Function to fully flatten nested JSON structures
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

# Flatten the data
flattened_records = [flatten_nested_fields(record) for record in data]
flattened_df = pd.DataFrame(flattened_records)

# Save to CSV
os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
flattened_df.to_csv(csv_file_path, index=False)

print(f"Flattened data saved to {csv_file_path}")
