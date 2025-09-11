import mysql.connector
import uuid
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

def create_visits_from_flat():
    conn = mysql.connector.connect(**DEST_DB)
    cursor = conn.cursor(dictionary=True)

    now = datetime.now()

    # Load visits from client_visits_flat which already has patient_id
    cursor.execute("SELECT * FROM tbl_vuci_visits_dates")
    visits = cursor.fetchall()

    for visit in visits:
        patient_id = visit.get('patient_id')
        if not patient_id:
            print(f"Skipping record with missing patient_id for client_id {visit.get('client_id')}")
            continue

        visit_type_id = 1
        location_id = 1
        date_started = visit.get('date_started') or now.date()
        date_stopped = visit.get('date_stopped') or date_started

        uuid_val = str(uuid.uuid4())

        cursor.execute("""
            INSERT INTO visit
            (patient_id, visit_type_id, date_started, date_stopped, location_id, creator, date_created, uuid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            patient_id,
            visit_type_id,
            date_started,
            date_stopped,
            location_id,
            1,
            now,
            uuid_val
        ))
        visit_id = cursor.lastrowid

        # Log into dreams_production.patient_visits_mapping
        cursor.execute("""
            INSERT INTO vuci_patient_visits_mapping (patient_id, visit_id)
            VALUES (%s, %s)
        """, (patient_id, visit_id))

        print(f"Inserted visit: patient_id {patient_id}, visit_date {date_started}")

    conn.commit()
    cursor.close()

if __name__ == "__main__":
    create_visits_from_flat()