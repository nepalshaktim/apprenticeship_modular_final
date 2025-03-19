from patient import generate_patient_data, default_serializer
from encounter import generate_encounter_data
from condition import generate_condition
import random
import os
import json



# Define file paths
desktop_path = os.path.expanduser("~/Desktop")
output_folder = os.path.join(desktop_path, "project_json/data_set7/raw_json")
os.makedirs(output_folder, exist_ok=True)
output_file_patient = os.path.join(output_folder, "patients.json")
output_file_encounter = os.path.join(output_folder, "encounters.json")
output_file_condition = os.path.join(output_folder, "conditions.json")

# Initialize dataset storage
patients = []
encounters = []
conditions = []

# Generate multiple patients
for _ in range(5000):
    birthday, today, deceased_date_time, patient_id, patient_data = generate_patient_data() # Generate a unique patient
    patients.append(patient_data)

    # Start encounter tracking from the patient's birthday
    encounter_start_min_date = None


    # Generate 1-5 encounters per patient
    for _ in range(random.choice([1,1,1,1,1,1,1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,5,5])):
        encounter_id, last_encounter_end_date, encounter_data = generate_encounter_data(birthday, today, deceased_date_time,patient_id, encounter_start_min_date)
        encounters.append(encounter_data)

        # Ensure the next encounter starts after this one
        encounter_start_min_date = last_encounter_end_date

        # Generate 1-3 conditions per encounter
        for _ in range(random.randint(1, 3)):
            conditions.append(generate_condition(birthday, today, deceased_date_time,patient_id, encounter_id))


# Save to JSON file
with open(output_file_patient, "w") as f:
    json.dump(patients, f, indent=4, default=default_serializer)

print(f" Patient records saved at: {output_file_patient}")

# Save encounters to JSON file
with open(output_file_encounter, "w") as f:
    json.dump(encounters, f, indent=4, default=default_serializer)

print(f" Encounter records saved at: {output_file_encounter}")

# Save conditions to JSON file
with open(output_file_condition, "w") as f:
    json.dump(conditions, f, indent=4, default=default_serializer)

print(f" Condition records saved at: {output_file_condition}")
