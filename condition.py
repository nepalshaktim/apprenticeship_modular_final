
import random
from random import randint
from datetime import date, timedelta
from faker import Faker

from fhir.resources.condition import Condition
from fhir.resources.identifier import Identifier
from fhir.resources.coding import Coding
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.reference import Reference

# Initialize Faker
fake = Faker()


CLINICAL_STATUS = [
    {'code': 'active', 'level': '1', 'display': 'Active'},
    {'code': 'recurrence', 'level': '2', 'display': 'Recurrence'},
    {'code': 'relapse', 'level': '2', 'display': 'Relapse'},
    {'code': 'inactive', 'level': '1', 'display': 'Inactive'},
    {'code': 'remission', 'level': '2', 'display': 'Remission'},
    {'code': 'resolved', 'level': '2', 'display': 'Resolved'},
    {'code': 'unknown', 'level': '1', 'display': 'Unknown'},
]
VERIFICATION_STATUS = [
    {'code': 'unconfirmed', 'level': '1', 'display': 'Unconfirmed'},
    {'code': 'provisional', 'level': '2', 'display': 'Provisional'},
    {'code': 'differential', 'level': '2', 'display': 'Differential'},
    {'code': 'confirmed', 'level': '1', 'display': 'Confirmed'},
    {'code': 'refuted', 'level': '1', 'display': 'Refuted'},
    {'code': 'entered-in-error', 'level': '1', 'display': 'Entered in Error'},
]

CATEGORY = [
    {'code': 'problem-list-item', 'display': 'Problem List I tem'},
    {'code': 'encounter-diagnosis', 'display': 'Encounter Diagnosis'}
]

SEVERITY = [
    {'code': '24484000', 'display': 'Severe'},
    {'code': '6736007', 'display': 'Moderate'},
    {'code': '255604002', 'display': 'Mild'}
]

CODE = [
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

BODY_SITE = [
    {'code': '53075003', 'display': 'Distal phalanx of hallux'},
    {'code': '3055008', 'display': 'Bone marrow of vertebral body'},
    {'code': '3964001', 'display': 'Gyrus of brain'},
    {'code': '344001', 'display': 'Ankle'},
    {'code': '691000', 'display': 'Small intestine submucosa'},
    {'code': '688000', 'display': 'Fetal hyaloid artery'},
    {'code': '4703008', 'display': 'Cardinal vein'},
    {'code': '5597008', 'display': 'Retina of right eye'},
    {'code': '7242000', 'display': 'Appendiceal muscularis propria'},
    {'code': '7756004', 'display': 'Lamina of third thoracic vertebra'},
    {'code': '8711009', 'display': 'Periodontal tissues'},
    {'code': '9796009', 'display': 'Skeletal muscle fiber, type IIb'}
]

PARTICIPANT_FUNCTION = [
    {'code': 'enterer', 'display': 'Enterer'},
    {'code': 'performer', 'display': 'Performer'},
    {'code': 'author', 'display': 'Author'},
    {'code': 'verifier', 'display': 'Verifier'},
    {'code': 'legal', 'display': 'Legal Authenticator'},
    {'code': 'attester', 'display': 'Attester'}
]

STAGE_SUMMARY = [
    {'code': '385356007', 'display': 'Tumor stage finding (finding)'},
    {'code': '2640006', 'display': 'Clinical stage IV'},
    {'code': '56769006', 'display': 'Modified Dukes stage A'},
    {'code': '385368003', 'display': 'FIGO stage finding for cervical carcinoma'},
    {'code': '394940002', 'display': 'Dukes stage B (finding)'},
    {'code': '396907008', 'display': 'Thymic epithelial neoplasm stage finding (finding)'},
    {'code': '405917009', 'display': 'Intergroup rhabdomyosarcoma study post-surgical clinical group finding (finding)'},
    {'code': '50283003', 'display': 'Clinical stage III'},
    {'code': '277772008', 'display': 'Node stage N1bi'},
    {'code': '53623008', 'display': 'N1 stage'}
]

STAGE_TYPE = [
    {'code': '261023001', 'display': 'Pathological staging (qualifier value)'},
    {'code': '260998006', 'display': 'Clinical staging (qualifier value)'},
    {'code': '254291000', 'display': 'Staging and scales'},
    {'code': '13808002', 'display': 'WR stage 3'},
    {'code': '134438001', 'display': 'Canadian Cardiovascular Society classification of angina'},
    {'code': '165270003', 'display': 'Physical disability assessment score'},
    {'code': '251896001', 'display': 'Breathlessness rating'},
    {'code': '254365003', 'display': 'Siopel liver staging system'},
    {'code': '254376004', 'display': 'Testicular tumor staging systems'},
    {'code': '258233007', 'display': 'Generic tumor staging descriptor (tumor staging)'}
]

def generate_condition(birthday,today, deceased_date_time, patient_id, encounter_id):

    condition_id = fake.uuid4()

    clinical_status_choice = random.choice(CLINICAL_STATUS)
    verification_status_choice = random.choice(VERIFICATION_STATUS)
    category_choice = random.choice(CATEGORY)
    severity_choice = random.choice(SEVERITY)
    code_choice = random.choice(CODE)
    body_site_choice = random.choice(BODY_SITE)
    participant_function_choice = random.choice(PARTICIPANT_FUNCTION)
    stage_summary_choice = random.choice(STAGE_SUMMARY)
    stage_type_choice = random.choice(STAGE_TYPE)

    def generate_condition_dates(birthday, today, deceased_date_time=None):
        """Generate valid onset, abatement, and recorded dates within birthday and deceased date (if applicable)."""

        max_valid_end_date = deceased_date_time if deceased_date_time else today

        # Ensure onset_date_time is within the valid range
        min_onset_offset = 3650  # Minimum 10 years after birthday
        max_onset_offset = min(18000, (max_valid_end_date - birthday).days)

        if min_onset_offset > max_onset_offset:
            onset_date_time = birthday  # If range is invalid, default to birthday
        else:
            onset_date_time = birthday + timedelta(days=randint(min_onset_offset, max_onset_offset),
                                                   hours=randint(0, 23),
                                                   minutes=randint(0, 59),
                                                   seconds=randint(0, 59),
                                                   microseconds=randint(0, 999999))

        # Ensure abatement_date_time follows onset_date_time
        min_abatement_offset = 365  # Minimum 1 year after onset_date_time
        max_abatement_offset = min(3650, (max_valid_end_date - onset_date_time).days)

        if min_abatement_offset > max_abatement_offset:
            abatement_date_time = onset_date_time  # Default to onset_date_time if range is invalid
        else:
            abatement_date_time = onset_date_time + timedelta(days=randint(min_abatement_offset, max_abatement_offset),
                                                              hours=randint(0, 23),
                                                              minutes=randint(0, 59),
                                                              seconds=randint(0, 59),
                                                              microseconds=randint(0, 999999)
                                                              )

        # Ensure recorded_date_time follows onset_date_time but is before abatement_date_time
        min_recorded_offset = 1  # Minimum 1 day after onset
        max_recorded_offset = min(364, (abatement_date_time - onset_date_time).days)

        if min_recorded_offset > max_recorded_offset:
            recorded_date_time = onset_date_time  # Default to onset_date_time if range is invalid
        else:
            recorded_date_time = onset_date_time + timedelta(days=randint(min_recorded_offset, max_recorded_offset),
                                                             hours=randint(0, 23),
                                                             minutes=randint(0, 59),
                                                             seconds=randint(0, 59),
                                                             microseconds=randint(0, 999999)
                                                             )

        return onset_date_time, abatement_date_time, recorded_date_time

    onset_date_time, abatement_date_time, recorded_date_time = generate_condition_dates(birthday, today, deceased_date_time)

    condition = Condition(
        resourceType="Condition",
        id=condition_id,
        identifier=[
            Identifier(
                use="official",
                system="http://hospital.smarthealthit.org",
                value=condition_id
            )
        ],
        clinicalStatus=CodeableConcept(
            coding=[Coding(
                system="http://terminology.hl7.org/CodeSystem/condition-clinical",
                version=f'Level: {clinical_status_choice["level"]}',
                code=clinical_status_choice["code"],
                display=clinical_status_choice["display"],

            )]
        ),
        verificationStatus=CodeableConcept(
            coding=[Coding(
                system="http://terminology.hl7.org/CodeSystem/condition-ver-status",
                version=f'Level: {verification_status_choice["level"]}',
                code=verification_status_choice["code"],
                display=verification_status_choice["display"],
            )]
        ),
        category=[CodeableConcept(
            coding=[Coding(
                system="http://terminology.hl7.org/CodeSystem/condition-category",
                code=category_choice["code"],
                display=category_choice["display"]
            )]
        )],
        severity=CodeableConcept(
            coding=[Coding(
                system="http://snomed.info/sct",
                code=severity_choice["code"],
                display=severity_choice["display"]
            )],
        ),
        code=CodeableConcept(
            coding=[Coding(
                system=code_choice["system"],
                code=code_choice["code"],
                display=code_choice["display"]
            )],
        ),
        bodySite=[CodeableConcept(
            coding=[Coding(
                system="http://snomed.info/sct",
                code=body_site_choice["code"],
                display=body_site_choice["display"]
            )]
        )],
        subject=Reference(
            reference=f"{patient_id}",
            #display="Patient Name"
        ),
        encounter=Reference(
            reference=f"{encounter_id}",
            display="Fake encounter id associated with this condition"
        ),
        onsetDateTime=onset_date_time.isoformat(),
        abatementDateTime=abatement_date_time.isoformat(),
        recordedDate=recorded_date_time.isoformat(),
        participant=[{
            "function": CodeableConcept(
                coding=[Coding(
                    system="http://terminology.hl7.org/CodeSystem/provenance-participant-type",
                    code=participant_function_choice["code"],
                    display=participant_function_choice["display"]
                )]
            ),
            "actor": Reference(
                reference=f"Practitioner/Dr. {fake.name()}")
        }],
        stage=[{
            "summary": CodeableConcept(
                coding=[Coding(
                    system="http://snomed.info/sct",
                    code=stage_summary_choice["code"],
                    display=stage_summary_choice["display"]
                )]
            ),
            "type": CodeableConcept(
                coding=[Coding(
                    system="http://snomed.info/sct	",
                    code=stage_type_choice["code"],
                    display=stage_type_choice["display"]
                )]
            ),

        }]
    )
    return condition.model_dump()