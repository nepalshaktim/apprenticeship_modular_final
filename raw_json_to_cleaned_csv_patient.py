import os
import pandas as pd
import json


# Define file paths
json_file_path = os.path.expanduser("~/Desktop/project_json/data_set7/raw_json/patients.json")
csv_file_path = os.path.expanduser("~/Desktop/project_csv/data_set7/cleaned_csv/patients.csv")

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

flattened_records = [flatten_nested_fields(patient) for patient in data]
flattened_df = pd.DataFrame(flattened_records)

# Define columns to keep and rename them
columns_to_keep = {
    "id": "patient_id",
    #"identifier_0.system": "identifier_use",
    "active": "active",
    #"name_0.family": "last_name",
    #"name_0.given_0": "first_name",
    #"name_0.period.start": "name_start",
    #"name_0.period.end": "name_end",
    #"telecom_0.system": "telecom_system",
    #"telecom_0.value": "telecom_value",
    #"telecom_0.use": "telecom_use",
    #"telecom_0.period.start": "telecom_start",
    #"telecom_0.period.end": "telecom_end",
    "gender": "gender",
    "birthDate": "birth_date",
    "deceasedBoolean": "deceased",
    "deceasedDateTime": "deceased_date",
    #"address_0.use": "address_use",
    #"address_0.type": "address_type",
    #"address_0.line_0": "street",
    #"address_0.city": "city",
    #"address_0.state": "state",
    #"address_0.postalCode": "zipcode",
    #"address_0.country": "country",
    #"address_0.period.start":"address_start",
    #"address_0.period.end": "address_end",
    #"maritalStatus.text": "marital_status",
    #"multipleBirthBoolean": "multiple_birth",
    #"multipleBirthInteger": "multiple_birth_integer",
    #"photo_0.contentType":"photo_type",
    #"photo_0.url": "photo_url",
    #"contact_0.relationship_0.text": "contact_relationship",
    #"contact_0.name.family": "contact_last_name",
    #"contact_0.name.given_0": "contact_first_name",
    #"contact_0.telecom_0.system": "contact_telecom_system",
    #"contact_0.telecom_0.value": "contact_telecom_value",
    #"contact_0.telecom_0.use": "contact_telecom_use",
    #"contact_0.address.use": "contact_address_use",
    #"contact_0.address.type": "contact_address_type",
    #"contact_0.address.line_0": "contact_street",
    #"contact_0.address.city": "contact_city",
    #"contact_0.address.state": "contact_state",
    #"contact_0.address.postalCode": "contact_zipcode",
    #"contact_0.address.country": "contact_country",
    #"contact_0.gender": "contact_gender",
    #"contact_0.organization.display": "contact_organization",
    #"communication_0.language.text": "communication_language",
    #"communication_0.preferred": "preferred",
    #"generalPractitioner_0.display": "general_practitioner",
    #"managingOrganization.display": "managing_organization"

}

# Filter and rename columns
filtered_df = flattened_df[list(columns_to_keep.keys())].rename(columns=columns_to_keep)

# Ensure the output folder exists
os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

# Save to CSV
filtered_df.to_csv(csv_file_path, index=False)

print(f"CSV file has been saved at: {csv_file_path}")
