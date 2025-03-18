import os
import pandas as pd
import json


# Define file paths
json_file_path = os.path.expanduser("~/Desktop/project_json/data_set7/raw_json/encounters.json")
csv_file_path = os.path.expanduser("~/Desktop/project_csv/data_set7/cleaned_csv/encounters.csv")

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
    "id": "encounter_id",
    "status": "status",
    "class_0.coding_0.code": "encounter_class_code",
    "class_0.coding_0.display": "encounter_class",
    "priority.coding_0.display": "encounter_priority",
    "type_0.coding_0.code": "encounter_type_code",
    "type_0.coding_0.display": "encounter_type",
    "serviceType_0.concept.coding_0.display": "service_type",
    "subject.reference": "patient_id",
    "subjectStatus.coding_0.display": "subject_status",
    "episodeOfCare_0.reference": "episode_of_care_reference",
    "basedOn_0.reference": "encounter_based_on",
    "careTeam_0.reference": "care_team",
    "partOf.reference": "part_of",
    "serviceProvider.reference": "service_provider",
    "participant_0.type_0.coding_0.display": "participant_type",
    "participant_0.period.start": "participant_start",
    "participant_0.period.end": "participant_end",
    "appointment_0.reference": "appointment_reference",
    "actualPeriod.start": "actual_start_date",
    "actualPeriod.end": "actual_end_date",
    "plannedStartDate": "planned_start_date",
    "plannedEndDate": "planned_end_date",
    "length.value": "length_of_stay",
    "length.unit": "length_unit",
    "diagnosis_0.condition_0.concept.coding_0.code": "diagnosis1_condition_code",
    "diagnosis_0.condition_0.concept.coding_0.display": "diagnosis1_condition",
    "diagnosis_0.use_0.coding_0.display": "diagnosis1_use",
    "diagnosis_1.condition_0.concept.coding_0.code": "diagnosis2_condition_code",
    "diagnosis_1.condition_0.concept.coding_0.display": "diagnosis2_condition",
    "diagnosis_1.use_0.coding_0.display": "diagnosis2_use",
    "diagnosis_2.condition_0.concept.coding_0.code": "diagnosis3_condition_code",
    "diagnosis_2.condition_0.concept.coding_0.display": "diagnosis3_condition",
    "diagnosis_2.use_0.coding_0.display": "diagnosis3_use",
    "account_0.reference": "account_reference",
    "dietPreference_0.coding_0.display": "diet_preference",
    "specialArrangement_0.coding_0.display": "special_arrangement",
    "specialCourtesy_0.coding_0.display": "special_courtesy",
    "admission.origin.reference": "admission_origin",
    "admission.admitSource.coding_0.display": "admission_source",
    "admission.reAdmission.coding_0.code": "readmission",
    "admission.destination.reference": "admission_destination",
    "admission.dischargeDisposition.coding_0.display": "discharge_disposition"

}

# Filter and rename columns
filtered_df = flattened_df[list(columns_to_keep.keys())].rename(columns=columns_to_keep)

# Ensure the output folder exists
os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

# Save to CSV
filtered_df.to_csv(csv_file_path, index=False)

print(f"CSV file has been saved at: {csv_file_path}")
