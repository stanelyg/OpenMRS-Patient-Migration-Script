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

def create_visits_from_flat():
    source_conn = mysql.connector.connect(**SOURCE_DB)
    dest_conn = mysql.connector.connect(**DEST_DB)

    src_cursor = source_conn.cursor(dictionary=True)
    dest_cursor = dest_conn.cursor()

    now = datetime.now()

    # Load visits from client_visits_flat which already has patient_id
    src_cursor.execute("SELECT * FROM client_visits_flat")
    visits = src_cursor.fetchall()

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

        dest_cursor.execute("""
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
        visit_id = dest_cursor.lastrowid

        # Log into dreams_production.patient_visits_mapping
        src_cursor.execute("""
            INSERT INTO dreams_production.patient_visits_mapping (patient_id, visit_id)
            VALUES (%s, %s)
        """, (patient_id, visit_id))

        print(f"Inserted visit: patient_id {patient_id}, visit_date {date_started}")

    dest_conn.commit()
    src_cursor.close()
    dest_cursor.close()
    source_conn.close()
    dest_conn.close()

if __name__ == "__main__":
    create_visits_from_flat()