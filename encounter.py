
from fhir.resources.encounter import Encounter
import random
from random import randint
from datetime import timedelta, timezone
from faker import Faker

from fhir.resources.identifier import Identifier
from fhir.resources.coding import Coding
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.reference import Reference
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.period import Period
from fhir.resources.duration import Duration

# Initialize Faker
fake = Faker()

CLASS_CODING = [
    {'code': 'IMP', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActCode', 'display': 'inpatient encounter'},
    {'code': 'AMB', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActCode','display': 'ambulatory'},
    {'code': 'OBSENC', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActCode', 'display': 'observation encounter'},
    {'code': 'EMER', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActCode', 'display': 'emergency'},
    {'code': 'VR', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActCode', 'display': 'virtual'},
    {'code': 'HH', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActCode', 'display': 'home health'},

]
PRIORITY = [
    {'code': 'A', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActPriority', 'display': 'ASAP'},
    {'code': 'CR', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActPriority','display': 'callback results'},
    {'code': 'EL', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActPriority', 'display': 'elective'},
    {'code': 'EM', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActPriority', 'display': 'emergency'},
    {'code': 'P', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActPriority', 'display': 'preop'},
    {'code': 'PRN', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActPriority', 'display': 'as needed'},
    {'code': 'R', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ActPriority', 'display': 'routine'},

]
TYPE = [
    {'code': 'ADMS', 'system': 'http://terminology.hl7.org/CodeSystem/encounter-type', 'display': 'Annual diabetes mellitus screening'},
    {'code': 'BD/BM-clin', 'system': 'http://terminology.hl7.org/CodeSystem/encounter-type', 'display': 'Bone drilling/bone marrow punction in clinic'},
    {'code': 'CCS60', 'system': 'http://terminology.hl7.org/CodeSystem/encounter-type', 'display': 'Infant colon screening - 60 minutes'},
    {'code': 'OKI', 'system': 'http://terminology.hl7.org/CodeSystem/encounter-type', 'display': 'Outpatient Kenacort injection'}

]

SERVICE_TYPE = [
    {'code': '1', 'system': 'http://terminology.hl7.org/CodeSystem/service-type', 'display': 'Adoption & permanent care information/support'},
    {'code': '2', 'system': 'http://terminology.hl7.org/CodeSystem/service-type', 'display': 'Aged care assessment'},
    {'code': '3', 'system': 'http://terminology.hl7.org/CodeSystem/service-type', 'display': 'Aged Care information/referral'},
    {'code': '4', 'system': 'http://terminology.hl7.org/CodeSystem/service-type', 'display': 'Aged Residential Care'},
    {'code': '5', 'system': 'http://terminology.hl7.org/CodeSystem/service-type', 'display': 'Case management for older persons'},
    {'code': '6', 'system': 'http://terminology.hl7.org/CodeSystem/service-type', 'display': 'Delivered meals (meals on wheels)'},
    {'code': '7', 'system': 'http://terminology.hl7.org/CodeSystem/service-type', 'display': 'Friendly visiting'},
    {'code': '8', 'system': 'http://terminology.hl7.org/CodeSystem/service-type', 'display': 'Home care/housekeeping assistance'},
    {'code': '9', 'system': 'http://terminology.hl7.org/CodeSystem/service-type', 'display': 'Home maintenance and repair'},

]
SUBJECT_STATUS = [
    {'code': 'arrived', 'system': 'http://terminology.hl7.org/CodeSystem/encounter-subject-status', 'display': 'Arrived'},
    {'code': 'triaged', 'system': 'http://terminology.hl7.org/CodeSystem/encounter-subject-status', 'display': 'Triaged'},
    {'code': 'receiving-care', 'system': 'http://terminology.hl7.org/CodeSystem/encounter-subject-status', 'display': 'Receiving Care'},
    {'code': 'on-leave', 'system': 'http://terminology.hl7.org/CodeSystem/encounter-subject-status', 'display': 'On Leave'},
    {'code': 'departed', 'system': 'http://terminology.hl7.org/CodeSystem/encounter-subject-status', 'display': 'Departed'}

]
PARTICIPANT_TYPE = [
    {'code': 'ADM', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ParticipationType', 'display': 'admitter'},
    {'code': 'ATND', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ParticipationType', 'display': 'attender'},
    {'code': 'CALLBCK', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ParticipationType', 'display': 'callback contact'},
    {'code': 'CON', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ParticipationType', 'display': 'consultant'},
    {'code': 'DIS', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ParticipationType', 'display': 'discharger'},
    {'code': 'ESC', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ParticipationType', 'display': 'escort'},
    {'code': 'REF', 'system': 'http://terminology.hl7.org/CodeSystem/v3-ParticipationType', 'display': 'referrer'},
    {'code': 'translator', 'system': 'http://terminology.hl7.org/CodeSystem/participant-type', 'display': 'Translator'},
    {'code': 'emergency', 'system': 'http://terminology.hl7.org/CodeSystem/participant-type', 'display': 'Emergency'},

]
DIAGNOSIS_USE = [
    {'code': 'working', 'system': 'http://hl7.org/fhir/encounter-diagnosis-use', 'display': 'Working'},
    {'code': 'final', 'system': 'http://hl7.org/fhir/encounter-diagnosis-use', 'display': 'Final'},
]

DIAGNOSIS_CONDITION1 = [
    {'code': '404684003', 'system': 'http://snomed.info/sct', 'display': 'Clinical finding (finding)'},
    {'code': '109006', 'system': 'http://snomed.info/sct', 'display': 'Anxiety disorder of childhood OR adolescence'},
    {'code': '122003', 'system': 'http://snomed.info/sct', 'display': 'Choroidal hemorrhage'},
    {'code': '127009', 'system': 'http://snomed.info/sct', 'display': 'Spontaneous abortion with laceration of cervix'},
    {'code': '129007', 'system': 'http://snomed.info/sct', 'display': 'Homoiothermia'},
    {'code': '134006', 'system': 'http://snomed.info/sct', 'display': 'Decreased hair growth'},
    {'code': '140004', 'system': 'http://snomed.info/sct', 'display': 'Chronic pharyngitis'},
    {'code': '144008', 'system': 'http://snomed.info/sct', 'display': 'Normal peripheral vision'},
    {'code': '150003', 'system': 'http://snomed.info/sct', 'display': 'Abnormal bladder continence'},

]
DIAGNOSIS_CONDITION2 = [
    {'code': '1531000', 'system': 'http://snomed.info/sct', 'display': 'Poisoning by nitrofuran derivatives'},
    {'code': '1532007', 'system': 'http://snomed.info/sct', 'display': 'Viral pharyngitis'},
    {'code': '1534008', 'system': 'http://snomed.info/sct', 'display': 'Palsy of conjugate gaze'},
    {'code': '1539003', 'system': 'http://snomed.info/sct', 'display': 'Acquired trigger finger'},
    {'code': '1544005', 'system': 'http://snomed.info/sct', 'display': 'Open dislocation of knee'},
    {'code': '1588003', 'system': 'http://snomed.info/sct', 'display': 'Heterosexual precocius puberty'},
    {'code': '1608005', 'system': 'http://snomed.info/sct', 'display': 'Increased capillary permeability'},
    {'code': '1654001', 'system': 'http://snomed.info/sct', 'display': 'Corticosteroid-induced glaucoma'},
    {'code': '1679003', 'system': 'http://snomed.info/sct', 'display': 'Arthritis associated with another disorder'},

]
DIAGNOSIS_CONDITION3 = [
    {'code': '1698001', 'system': 'http://snomed.info/sct', 'display': 'Ulcer of bile duct'},
    {'code': '1739001', 'system': 'http://snomed.info/sct', 'display': 'Fracture of occipital bone'},
    {'code': '1763009', 'system': 'http://snomed.info/sct', 'display': 'Interstitial keratitis'},
    {'code': '1771008', 'system': 'http://snomed.info/sct', 'display': 'Insulin biosynthesis defect'},
    {'code': '2506003', 'system': 'http://snomed.info/sct', 'display': 'Early onset dysthymia'},
    {'code': '2523007', 'system': 'http://snomed.info/sct', 'display': 'Salmonella pneumonia'},
    {'code': '2772003', 'system': 'http://snomed.info/sct', 'display': 'Acquired epidermolysis bullosa'},
    {'code': '2819007', 'system': 'http://snomed.info/sct', 'display': 'Poisoning by magnesium sulfate'},
    {'code': '2919008', 'system': 'http://snomed.info/sct', 'display': 'Nausea, vomiting and diarrhea'},

]
DIET_PREFERENCE = [
    {'code': 'vegetarian', 'system': 'http://terminology.hl7.org/CodeSystem/diet', 'display': 'Vegetarian'},
    {'code': 'dairy-free', 'system': 'http://terminology.hl7.org/CodeSystem/diet', 'display': 'Dairy Free'},
    {'code': 'nut-free', 'system': 'http://terminology.hl7.org/CodeSystem/diet', 'display': 'Nut Free'},
    {'code': 'gluten-free', 'system': 'http://terminology.hl7.org/CodeSystem/diet', 'display': 'Gluten Free'},
    {'code': 'vegan', 'system': 'http://terminology.hl7.org/CodeSystem/diet', 'display': 'Vegan'},
    {'code': 'halal', 'system': 'http://terminology.hl7.org/CodeSystem/diet', 'display': 'Halal'},
    {'code': 'kosher', 'system': 'http://terminology.hl7.org/CodeSystem/diet', 'display': 'Kosher'}

]
ADMIT_SOURCE = [
    {'code': 'hosp-trans', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'Transferred from other hospital'},
    {'code': 'emd', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'From accident/emergency department'},
    {'code': 'outp', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'From outpatient department'},
    {'code': 'born', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'Born in hospital'},
    {'code': 'gp', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'General Practitioner referral'},
    {'code': 'mp', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'Medical Practitioner/physician referral'},
    {'code': 'nursing', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'From nursing home'},
    {'code': 'psych', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'From psychiatric hospital'},
    {'code': 'rehab', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'From rehabilitation facility'},
    {'code': 'other', 'system': 'http://terminology.hl7.org/CodeSystem/admit-source', 'display': 'Other'},
]
DISCHARGE_DISPOSITION = [
    {'code': 'home', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Home'},
    {'code': 'alt-home', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Alternative home'},
    {'code': 'other-hcf', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Other healthcare facility'},
    {'code': 'hosp', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Hospice'},
    {'code': 'long', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Long-term care'},
    {'code': 'aadvice', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Left against advice'},
    {'code': 'exp', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Expired'},
    {'code': 'psy', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Psychiatric hospital'},
    {'code': 'rehab', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Rehabilitation'},
    {'code': 'snf', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Skilled nursing facility'},
    {'code': 'oth', 'system': 'http://terminology.hl7.org/CodeSystem/discharge-disposition', 'display': 'Other'},

]


def generate_encounter_data(birthday, today, deceased_date_time, patient_id, encounter_start_min_date):
    """Generate a unique FHIR-compliant Encounter resource linked to a patient."""

    encounter_id = fake.uuid4()  # Unique encounter ID

    class_choice = random.choice(CLASS_CODING)
    priority_choice = random.choice(PRIORITY)
    type_choice = random.choice(TYPE)
    service_type_choice = random.choice(SERVICE_TYPE)
    subject_status_choice = random.choice(SUBJECT_STATUS)
    participant_type_choice = random.choice(PARTICIPANT_TYPE)
    diagnosis_use_choice1 = random.choice(DIAGNOSIS_USE)
    diagnosis_use_choice2 = random.choice(DIAGNOSIS_USE)
    diagnosis_use_choice3 = random.choice(DIAGNOSIS_USE)
    diagnosis_condition1_choice = random.choice(DIAGNOSIS_CONDITION1)
    diagnosis_condition2_choice = random.choice(DIAGNOSIS_CONDITION2)
    diagnosis_condition3_choice = random.choice(DIAGNOSIS_CONDITION3)
    diet_preference_choice = random.choice(DIET_PREFERENCE)
    admit_source_choice = random.choice(ADMIT_SOURCE)
    discharge_disposition_choice = random.choice(DISCHARGE_DISPOSITION)

    # Function to ensure a date range is between birthday and deceased_date_time (if exists)
    def generate_valid_event_timeframes(birthday, today, deceased_date_time=None, encounter_start_min_date=None):
        """Ensure participant and actual and planned encounter times are valid within birthday and deceased date (if applicable)."""

        max_valid_end_date = deceased_date_time if deceased_date_time else today

        # Ensure first encounter does not go too far back and encounter start time is within valid range
        if encounter_start_min_date is None:
            encounter_start_min_date = max(birthday, today - timedelta(days=375))
        encounter_start_max_date = min(max_valid_end_date, today - timedelta(days=10))

        # Ensure the date range is valid
        if encounter_start_min_date > encounter_start_max_date:
            encounter_start_min_date = encounter_start_max_date  # Prevents invalid range


        actual_encounter_start_time = fake.date_time_between(start_date=encounter_start_min_date,
                                                        end_date=encounter_start_max_date).replace(tzinfo=timezone.utc)

        # Ensure encounter end time follows start time
        actual_encounter_end_time = actual_encounter_start_time + timedelta(days=randint(1, 10),
                                                                            hours=randint(0, 23),
                                                                            minutes=randint(0, 59),
                                                                            seconds=randint(0, 59),
                                                                            microseconds=randint(0, 999999))

        # Calculate encounter length
        actual_encounter_length = (actual_encounter_end_time - actual_encounter_start_time).days

        return  actual_encounter_start_time, actual_encounter_end_time, actual_encounter_length

    # Generate participant and encounter timeframes
    actual_encounter_start_time, actual_encounter_end_time, actual_encounter_length = generate_valid_event_timeframes(birthday, today, deceased_date_time, encounter_start_min_date)
    participant_start_time = (actual_encounter_start_time + timedelta(days=randint(1, 3),
                                                                      hours=randint(0, 23),
                                                                      minutes=randint(0, 59),
                                                                      seconds=randint(0, 59),
                                                                      microseconds=randint(0, 999999)
                                                                      ))
    participant_end_time = (participant_start_time + timedelta(days=randint(1, 6),
                                                               hours=randint(0, 23),
                                                               minutes=randint(0, 59),
                                                               seconds=randint(0, 59),
                                                               microseconds=randint(0, 999999)
                                                               ))
    planned_start_date = (actual_encounter_start_time + timedelta(days=randint(-3, 3),
                                                                  hours=randint(0, 23),
                                                                  minutes=randint(0, 59),
                                                                  seconds=randint(0, 59),
                                                                  microseconds=randint(0, 999999)
                                                                  ))
    planned_end_date = (planned_start_date + timedelta(days=randint(0, 6),
                                                       hours=randint(0, 23),
                                                       minutes=randint(0, 59),
                                                       seconds=randint(0, 59),
                                                       microseconds=randint(0, 999999)
                                                       ))


    encounter = Encounter(
        resourceType="Encounter",
        id=encounter_id,  # Unique encounter ID
        identifier=[
            Identifier(
                use="official",
                system="http://hospital.smarthealthit.org",
                value=encounter_id  # Unique identifier
            )
        ],
        status=random.choice(["planned", "in-progress", "on-hold", "discharged", "completed", "cancelled", "discontinued", "entered-in-error", "unknown"]),
        class_fhir=[{
            "coding": [Coding(
                system=class_choice["system"],
                code=class_choice["code"],
                display=class_choice["display"]
            )]
        }],
        priority=CodeableConcept(
            coding=[Coding(
                system=priority_choice["system"],
                code=priority_choice["code"],
                display=priority_choice["display"]
            )]
        ),
        type=[CodeableConcept(
            coding=[Coding(
                system=type_choice["system"],
                code=type_choice["code"],
                display=type_choice["display"]
            )]
        )],
        serviceType=[
            CodeableReference(
                concept=CodeableConcept(
                    coding=[
                        Coding(
                            system=service_type_choice["system"],
                            code=service_type_choice["code"],
                            display=service_type_choice["display"]
                        )
                    ]
                )
            )
        ],
        subject=Reference(
            reference=f"{patient_id}",
            #display=fake.name()
        ),
        subjectStatus=CodeableConcept(
            coding=[Coding(
                system=subject_status_choice["system"],
                code=subject_status_choice["code"],
                display=subject_status_choice["display"]
            )]
        ),
        episodeOfCare=[Reference(
            reference=f"EpisodeOfCare/{fake.name()}",
            type=fake.uri(),
            display=f"This is episode number {fake.random_int(1, 10)} of care"
        )
        ],
        basedOn=[Reference(
            reference=f'{random.choice(["CarePlan", "DeviceRequest", "ImmunizationRecommendation", "MedicationRequest", "NutritionOrder", "RequestOrchestration", "ServiceRequest", "VisionPrescription"])} initiated this encounter',
        )
        ],
        careTeam=[
            Reference(
                reference=f"The group led by Dr. {fake.name()} is allocated to participate in this encounter"
            )
        ],
        partOf=Reference(
            reference=f"This Encounter is part of another encounter: {fake.uuid4()}"
        ),
        serviceProvider=Reference(
            reference=random.choice(
                ["Emergency", "Surgery", "Cardiology", "Pediatrics", "Oncology", "Neurology", "ICU", "ENT"])
        ),
        participant=[{
            "type": [CodeableConcept(
                coding=[Coding(
                    system=participant_type_choice["system"],
                    code=participant_type_choice["code"],
                    display=participant_type_choice["display"]
                )]
            )],
            "period": Period(
                start=participant_start_time.isoformat(),
                end=participant_end_time.isoformat()
            ),
            "actor": Reference(
                reference=f"Practitioner/{fake.uuid4()}")
        }],
        appointment=[Reference(
            reference=f"Appointment that scheduled this encounter/{fake.uuid4()}"
        )],

        actualPeriod=Period(
            start=actual_encounter_start_time.isoformat(),
            end=actual_encounter_end_time.isoformat()
        ),
        plannedStartDate=planned_start_date.isoformat(),
        plannedEndDate=planned_end_date.isoformat(),
        length=Duration(
            value=actual_encounter_length,
            unit="days"
        ),

        reason=[{
            "value": [{
                "concept": {
                    "text": fake.sentence()
                }
            }]
        }],
        diagnosis=[
            {
                "condition": [{
                    "concept": {
                        "coding": [Coding(
                            system=diagnosis_condition1_choice["system"],
                            code=diagnosis_condition1_choice["code"],
                            display=diagnosis_condition1_choice["display"]
                        )

                        ]
                    },
                    "reference": {
                        "reference": f"Condition/{fake.uuid4()}"
                    }
                }],
                "use": [CodeableConcept(
                    coding=[Coding(
                        system=diagnosis_use_choice1["system"],
                        code=diagnosis_use_choice1["code"],
                        display=diagnosis_use_choice1["display"]
                    )]
                )]
            },
            {
                "condition": [{
                    "concept": {
                        "coding": [Coding(
                            system=diagnosis_condition2_choice["system"],
                            code=diagnosis_condition2_choice["code"],
                            display=diagnosis_condition2_choice["display"]
                        )

                        ]
                    },
                    "reference": {
                        "reference": f"Condition/{fake.uuid4()}"
                    }
                }],
                "use": [CodeableConcept(
                    coding=[Coding(
                        system=diagnosis_use_choice2["system"],
                        code=diagnosis_use_choice2["code"],
                        display=diagnosis_use_choice2["display"]
                    )]
                )]
            },
            {
                "condition": [{
                    "concept": {
                        "coding": [Coding(
                            system=diagnosis_condition3_choice["system"],
                            code=diagnosis_condition3_choice["code"],
                            display=diagnosis_condition3_choice["display"]
                        )

                        ]
                    },
                    "reference": {
                        "reference": f"Condition/{fake.uuid4()}"
                    }
                }],
                "use": [CodeableConcept(
                    coding=[Coding(
                        system=diagnosis_use_choice3["system"],
                        code=diagnosis_use_choice3["code"],
                        display=diagnosis_use_choice3["display"]
                    )]
                )]
            }

        ],
        account=[Reference(
            reference=f"Account/{fake.name()}"
        )],
        dietPreference=[CodeableConcept(
            coding=[Coding(
                system=diet_preference_choice["system"],
                code=diet_preference_choice["code"],
                display=diet_preference_choice["display"]
            )]
        )],
        specialArrangement=[CodeableConcept(
            coding=[Coding(
                system="http://terminology.hl7.org/CodeSystem/encounter-special-arrangements",
                code=random.choice(["1", "2", "3", "4", "5"]),
                display=random.choice(["Wheelchair", "Additional bedding", "Interpreter", "Attendant", "Guide dog"])
            )]
        )],
        specialCourtesy=[CodeableConcept(
            coding=[Coding(
                system="http://terminology.hl7.org/CodeSystem/encounter-special-arrangements",
                code=random.choice(["1", "2", "3", "4", "5", "6"]),
                display=random.choice(["extended courtesy", "normal courtesy", "professional courtesy", "staff", "very important person", "unknown"])
            )]
        )],
        admission={
            "origin": Reference(
                reference=f"{fake.first_name()} Medical Center, {fake.address()}"
            ),
            "admitSource": CodeableConcept(
                coding=[Coding(
                    system=admit_source_choice["system"],
                    code=admit_source_choice["code"],
                    display=admit_source_choice["display"]
                )]
            ),
            "reAdmission": CodeableConcept(
                coding=[Coding(
                    system="http://terminology.hl7.org/CodeSystem/v2-0092",
                    code=f"{fake.boolean(15)}",
                    display="Re-admission"
                )]
            ),
            "destination": Reference(
                reference=f"{fake.last_name()} Treatment Center, {fake.address()}"
            ),
            "dischargeDisposition": CodeableConcept(
                coding=[Coding(
                    system=discharge_disposition_choice["system"],
                    code=discharge_disposition_choice["code"],
                    display=discharge_disposition_choice["display"]
                )]
            )
        }

    )

    return encounter_id, actual_encounter_end_time, encounter.model_dump()



