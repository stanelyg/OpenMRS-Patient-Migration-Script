import mysql.connector
import uuid
import argparse
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
# Destination DB (OpenMRS)
DEST_DB = {
    'host': os.getenv('DEST_DB_HOST'),
    'user': os.getenv('DEST_DB_USER'),
    'password': os.getenv('DEST_DB_PASSWORD'),
    'database': os.getenv('DEST_DB_NAME')
}

# Command-line args for form_id and encounter_type
parser = argparse.ArgumentParser(description="Insert encounters into OpenMRS.")
parser.add_argument('--form_id', type=int, required=True, help='OpenMRS Form ID')
parser.add_argument('--encounter_type', type=int, required=True, help='Encounter Type ID')
args = parser.parse_args()

form_id = args.form_id
encounter_type = args.encounter_type

def insert_encounters():
    conn = mysql.connector.connect(**DEST_DB)
    cursor = conn.cursor(dictionary=True)
    now = datetime.now()

    # Get data from joined table that includes visit_id and patient_id
    cursor.execute("""
    SELECT m.patient_id,m.visit_id,v.date_started FROM dreams_patient_visits_mapping m 
    INNER JOIN visit v ON v.visit_id=m.visit_id 
        """)
    records =cursor.fetchall()

    for rec in records:
        patient_id = rec['patient_id']
        visit_id = rec['visit_id']
        encounter_datetime = rec['date_started'] or now.date()

        uuid_val = str(uuid.uuid4())

        cursor.execute("""
            INSERT INTO encounter
            (encounter_datetime, patient_id, encounter_type, form_id, visit_id, location_id, creator, date_created, uuid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            encounter_datetime,
            patient_id,
            encounter_type,
            form_id,
            visit_id,
            1,          # location_id
            1,          # creator
            now,
            uuid_val
        ))
        encounter_id = cursor.lastrowid
        # insert the encounter provider 
        cursor.execute("""
            INSERT INTO encounter_provider (
                encounter_id, provider_id, encounter_role_id,
                creator, date_created, uuid
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            encounter_id,
            1,
            1,
            1,              # creator
            now,
            str(uuid.uuid4())
        ))

        # Log into dreams_production.`service_uptake_encounter_mapping`
        cursor.execute("""
            INSERT INTO behavioural_encounter_mapping2 (patient_id, encounter_id)
            VALUES (%s, %s)
        """, (patient_id, encounter_id))

        print(f"Inserted encounter for patient {patient_id} linked to visit {encounter_id}")

    conn.commit()
    cursor.close()

if __name__ == '__main__':
    insert_encounters()
