from turtledemo.penrose import start

from fhir.resources.patient import Patient
from fhir.resources.humanname import HumanName
from fhir.resources.contactpoint import ContactPoint
from fhir.resources.address import Address
from fhir.resources.attachment import Attachment

import uuid
import os
import json
import random
from random import randint
from datetime import date, timedelta, datetime, timezone
from faker import Faker

from fhir.resources.condition import Condition
from fhir.resources.identifier import Identifier
from fhir.resources.coding import Coding
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.reference import Reference
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.period import Period
from fhir.resources.duration import Duration

# Initialize Faker
fake = Faker()

def default_serializer(obj):
    """Custom serializer for non-serializable objects."""
    if hasattr(obj, "model_dump"):
        # Handle FHIR resources that have model_dump method
        return obj.model_dump()
    elif hasattr(obj, "isoformat"):
        # Handle datetime objects
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def generate_patient_data():
    """Generate a unique FHIR-compliant Patient resource."""
    patient_id = fake.uuid4()  # Unique ID for each patient

    # Generate birthday as a datetime ensuring the person is between 0 and 100 years old
    birthday = fake.date_time_between(start_date="-100y", end_date="now").replace(tzinfo=timezone.utc)  # Full datetime

    # Today's date
    today = datetime.today().replace(tzinfo=timezone.utc)

    # Function to generate valid start and end times
    def generate_valid_timeframes(start_reference):
        """Generate start and end times ensuring end_time does not exceed today."""
        max_start_time_offset = min(3650, (today - start_reference).days)  # Ensure within range
        start_time = start_reference + timedelta(days=randint(1, max_start_time_offset),
                                                 hours=randint(0,23),
                                                 minutes=randint(0,59),
                                                 seconds=randint(0,59),
                                                 microseconds=randint(0,999999))
        start_time = start_time.replace(tzinfo=timezone.utc)  # Ensure timezone is set

        max_end_time_offset = max(1, (today - start_time).days ) # Max days possible until today, ensure at least 1 day offset

        # Ensure end_time is between start_time and today
        end_time = start_time + timedelta(days=randint(0, max_end_time_offset),
                                          hours=randint(0, 23),
                                          minutes=randint(0, 59),
                                          seconds=randint(0, 59),
                                          microseconds=randint(0, 999999))  # Can be same day
        end_time = end_time.replace(tzinfo=timezone.utc)  # Ensure timezone is set

        return start_time, end_time

    # Generate valid timeframes for different attributes
    human_name_start_time, human_name_end_time = generate_valid_timeframes(birthday)
    telecom_start_time, telecom_end_time = generate_valid_timeframes(birthday)
    address_start_time, address_end_time = generate_valid_timeframes(birthday)
    contact_start_time, contact_end_time = generate_valid_timeframes(birthday)

    # Determine the latest end date
    latest_end_date = max(human_name_end_time, telecom_end_time, address_end_time, contact_end_time)

    # Generate deceased date ensuring it's after the latest end date but not beyond today
    # min_deceased_offset = (latest_end_date - birthday).days  # Ensure it comes after all end dates
    max_deceased_offset = (today - latest_end_date).days  # Max days possible until today

    # Ensure deceased date is valid
    if max_deceased_offset < 0:
        deceased_date_time1 = latest_end_date  # If today is before latest_end_date, use latest_end_date
    else:
        deceased_date_time1 = latest_end_date + timedelta(days=randint(0, max_deceased_offset),
                                                          hours=randint(0, 23),
                                                          minutes=randint(0, 59),
                                                          seconds=randint(0, 59),
                                                          microseconds=randint(0, 999999)
                                                          )
        deceased_date_time1 = deceased_date_time1.replace(tzinfo=timezone.utc)  # Ensure timezone is set

    # Logic for generating deceasedBoolean and deceasedDateTime
    def generate_deceased_data():
        """Generate deceasedBoolean and deceasedDateTime."""
        deceased_boolean = fake.boolean(chance_of_getting_true=10)  # 10% chance of being True
        deceased_date_time = None

        if deceased_boolean:
            deceased_date_time = deceased_date_time1
        return deceased_boolean, deceased_date_time

    # Generate deceased data
    deceased_boolean, deceased_date_time = generate_deceased_data()

    # Generate multipleBirthBoolean with a 5% chance of being True
    multiple_birth_boolean = fake.boolean(chance_of_getting_true=5)

    # Initialize multiple_birth_integer as None
    multiple_birth_integer = None

    # If multipleBirthBoolean is True, generate a multipleBirthInteger
    if multiple_birth_boolean:
        multiple_birth_integer = random.choice([2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 5])

    patient = Patient(
        resourceType="Patient",
        id=patient_id,  # Unique patient ID
        identifier=[
            Identifier(
                use="official",
                system="http://hospital.smarthealthit.org",
                value=str(uuid.uuid4())  # Unique identifier
            )
        ],
        active=fake.boolean(),
        name=[
            HumanName(
                use=fake.random_element(["usual", "official", "temp", "nickname", "anonymous", "old", "maiden"]),
                family=fake.last_name(),
                given=[fake.first_name()],
                prefix=[fake.prefix()],
                suffix=[fake.suffix()],
                period=Period(
                    start=human_name_start_time,
                    end=human_name_end_time
                )

            )
        ],
        telecom=[
            ContactPoint(
                system=fake.random_element(["phone", "fax", "email", "pager", "url", "sms", "other"]),
                value=fake.phone_number(),
                use=fake.random_element(["home", "work", "temp", "old", "mobile"]),
                rank=fake.random_element([1, 1, 1, 2, 3, 4]),
                period=Period(
                    start=telecom_start_time,
                    end=telecom_end_time

                )
            )
        ],
        gender=fake.random_element(["male", "female", "other", "unknown"]),
        birthDate=birthday.date(),
        deceasedBoolean=deceased_boolean if not deceased_date_time else None,
        deceasedDateTime=deceased_date_time,
        address=[
            Address(
                use=fake.random_element(["home", "work", "temp", "old", "billing"]),
                type=fake.random_element(["postal", "physical", "both"]),
                text="Text representation of the address",
                line=[fake.street_address()],
                city=fake.city(),
                state=fake.state_abbr(),
                postalCode=fake.zipcode(),
                country=fake.country(),
                period=Period(
                    start=address_start_time,
                    end=address_end_time

                )
            )
        ],
        maritalStatus=CodeableConcept(
            text=fake.random_element(
                ["Single", "Married", "Divorced", "Widowed", "Annulled", "Interlocutory", "Legally Separated",
                 "Common Law", "Domestic partner", "Never Married", "unknown"])
        ),
        multipleBirthBoolean=multiple_birth_boolean if not multiple_birth_integer else None,
        multipleBirthInteger=multiple_birth_integer,
        photo=[
            Attachment(
                contentType="image/jpeg",
                language=fake.language_name(),
                # data=fake.random_int(),
                url=fake.image_url(),
                size=fake.random_int(1, 1024),
                # hash=fake.random_int(),
                title=f"Photo of Patient",
                creation=fake.date(),
                height=fake.random_int(1, 1024),
                width=fake.random_int(1, 1024),
                frames=fake.random_int(1, 1024),
                duration=fake.pydecimal(),
                pages=fake.random_int(1, 1024),

            )
        ],
        contact=[
            {
                "relationship": [
                    CodeableConcept(
                        coding=[
                            Coding(
                                system=fake.uri(),
                                version="Version of the system - if applicable",
                                code=fake.random_element(),
                                display="Representation defined by the system",
                                userSelected=fake.boolean()

                            )
                        ],
                        text=fake.random_element(
                            ["Spouse", "Child", "Parent", "Sibling", "Guardian", "Friend", "Other"]),
                    )
                ],
                # role field was removed because of the errors and not being able to fix it
                "name": HumanName(
                    use=fake.random_element(["usual", "official", "temp", "nickname", "anonymous", "old", "maiden"]),
                    # text=f"Text representation of the full name",
                    family=fake.last_name(),
                    given=[fake.first_name()],
                    prefix=[fake.prefix()],
                    suffix=[fake.suffix()],
                    period=Period(
                        start=contact_start_time,
                        end=contact_end_time
                    ),

                ),
                # additionalName not used for the scope of project
                "telecom": [
                    ContactPoint(
                        system=fake.random_element(["phone", "fax", "email", "pager", "url", "sms", "other"]),
                        value=fake.phone_number(),
                        use=fake.random_element(["home", "work", "temp", "old", "mobile"]),
                        rank=fake.random_element([1, 1, 1, 2, 3, 4]),
                        period=Period(
                            start=fake.date_time(),
                            end=fake.date_time()

                        ),
                    ),
                ],
                "address": Address(
                    use=fake.random_element(["home", "work", "temp", "old", "billing"]),
                    type=fake.random_element(["postal", "physical", "both"]),
                    # text="Text representation of the address",
                    line=[fake.street_address()],
                    city=fake.city(),
                    state=fake.state_abbr(),
                    postalCode=fake.zipcode(),
                    country=fake.country(),
                    period=Period(
                        start=fake.date_time(),
                        end=fake.date_time()

                    )
                ),
                # additionalAddress not used
                "gender": fake.random_element(["male", "female", "other", "unknown"]),
                "organization": Reference(
                    display=fake.company()
                ),
                "period": Period(
                    start=fake.date_time(),
                    end=fake.date_time()
                )
            }
        ],
        communication=[
            {
                "language": CodeableConcept(
                    text=fake.language_name()
                ),
                "preferred": fake.boolean()
            }
        ],
        generalPractitioner=[
            Reference(
                display=f"Dr. {fake.last_name()} {fake.first_name()}"
            )
        ],
        managingOrganization=Reference(
            display=fake.company()
        )
    )

    return birthday, today, deceased_date_time, patient_id, patient.model_dump()  # Return both patient ID and JSON data

