import mysql.connector
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Source DB (client data)
SOURCE_DB = {
    'host': os.getenv('SOURCE_DB_HOST'),
    'user': os.getenv('SOURCE_DB_USER'),
    'password': os.getenv('SOURCE_DB_PASSWORD'),
    'database': os.getenv('SOURCE_DB_NAME')
}

# Destination DB (OpenMRS)
DEST_DB = {
    'host': os.getenv('DEST_DB_HOST'),
    'user': os.getenv('DEST_DB_USER'),
    'password': os.getenv('DEST_DB_PASSWORD'),
    'database': os.getenv('DEST_DB_NAME')
}

# Constants
CREATOR_ID = 1
LOCATION_ID = 1


def migrate_patients():
    source_conn = mysql.connector.connect(**SOURCE_DB)
    dest_conn = mysql.connector.connect(**DEST_DB)

    source_cursor = source_conn.cursor(dictionary=True)
    dest_cursor = dest_conn.cursor()

    source_cursor.execute("SELECT * FROM client_flat where client_id =2") 
    patients = source_cursor.fetchall()
    for row in patients:
        now = datetime.now()
        person_uuid = str(uuid.uuid4())

        # Insert into person
        dest_cursor.execute("""
            INSERT INTO person (gender, birthdate, birthdate_estimated, dead, creator, date_created, uuid)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['gender'][0].upper(),
            row['birthdate'],
            int(row.get('birthdate_estimated', 0)),
            int(row.get('dead', 0)),
            CREATOR_ID,
            now,
            person_uuid
        ))

        person_id = dest_cursor.lastrowid

        # Insert into person_name
        dest_cursor.execute("""
            INSERT INTO person_name (person_id, given_name, middle_name, family_name, preferred, creator, date_created, uuid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            person_id,
            row.get('given_name'),
            row.get('middle_name'),
            row.get('family_name'),
            int(row.get('preferred', 1)),
            CREATOR_ID,
            now,
            str(uuid.uuid4())
        ))

        # Insert into person_address
        dest_cursor.execute("""
            INSERT INTO person_address (person_id, address1, address2, city_village, county_district, state_province, address4, uuid, creator, date_created)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            person_id,
            row.get('address2'),
            row.get('address5'),
            row.get('city_village'),
            row.get('county_district'),
            row.get('state_province'),
            row.get('address4'),
            str(uuid.uuid4()),
            CREATOR_ID,
            now
        ))

        # Insert person_attribute(s)
        attributes = {
            17: row.get('Guardian_First_Name'),
            18: row.get('Guardian_last_name'),
            8: row.get('telephone_number'),
        }

        for attr_type_id, value in attributes.items():
            if value:
                dest_cursor.execute("""
                    INSERT INTO person_attribute (person_id, value, person_attribute_type_id, creator, date_created, uuid)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (person_id, value, attr_type_id, 1, now, str(uuid.uuid4())))
        
        # patient
        dest_cursor.execute("""
            INSERT INTO patient (patient_id, creator, date_created)
            VALUES (%s, %s, %s)
        """, (person_id, CREATOR_ID, now))

        # Insert patient identifiers if provided
        identifiers = {
            5: row.get('Birth_Certificate_Number'),
            27: row.get('National_ID'),
        }

        identifier_inserted = False
        for identifier_type_id, value in identifiers.items():
            if value:
                trimmed_identifier = value.strip()[:50]
                dest_cursor.execute("""
                    INSERT INTO patient_identifier
                    (patient_id, identifier, identifier_type, location_id, preferred, creator, date_created, uuid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (person_id, trimmed_identifier, identifier_type_id, 1, 1, 1, now, str(uuid.uuid4())))
                identifier_inserted = True
                break  # Only insert one valid identifier

        # Fallback: Generate OpenMRS-style identifier if none was available
        if not identifier_inserted:
            fallback_id = f"PT{person_id:06}" # Or use person_id-based
            dest_cursor.execute("""
                INSERT INTO patient_identifier
                (patient_id, identifier, identifier_type, location_id, preferred, creator, date_created, uuid)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (person_id, fallback_id, 1, 1, 1, 1, now, str(uuid.uuid4())))
        
        # map client to patient
        dest_cursor.execute("""
                   INSERT INTO  dreams_client_patient_mapping (client_id,patient_id)
                   VALUES (%s,%s)""",(row.get('client_id'),person_id))

    

        print(f"Migrated:(person_id {person_id})")

    dest_conn.commit()
    source_cursor.close()
    dest_cursor.close()
    source_conn.close()
    dest_conn.close()

if __name__ == "__main__":
    migrate_patients()



