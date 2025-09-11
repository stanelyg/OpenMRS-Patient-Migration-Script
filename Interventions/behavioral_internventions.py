
import pandas as pd
import mysql.connector
import uuid
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'user': 'henryg',
    'password': 'P@ssw0rd@1234',
    'database': 'openmrs'
}

concept_map = {
    "intervention_type_id": {"concept_id": 1000880, "type": "coded"},
    "intervention_date": {"concept_id": 1000884, "type": "date"},
    "comment": {"concept_id": 1000653, "type": "text"}
}

def load_value_map(cursor, table_name):
    cursor.execute(f"SELECT id, concept_id FROM {table_name}")
    return {str(row['id']): row['concept_id'] for row in cursor.fetchall()}

def get_patient_id(cursor, client_id):
    cursor.execute("""
        SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id = %s
    """, (client_id,))
    row = cursor.fetchone()
    return row['patient_id'] if row and 'patient_id' in row else None

def create_encounter(cursor, patient_id):
    now = datetime.now()
    cursor.execute("""
    SELECT m.patient_id,m.visit_id,v.date_started FROM Nuru_visits_mapping m 
    INNER JOIN visit v ON v.visit_id=m.visit_id 
                   Where m.patient_id=1153681              ;       
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
            480,
            550,
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
    return encounter_id if encounter_id else None


def insert_obs(cursor, person_id, encounter_id, concept_id, value, value_type, field_name):
    if value is None or value == "":
        return
    obs_uuid = str(uuid.uuid4())
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    field_map = {
        "coded": "value_coded",
        "text": "value_text",
        "date": "value_datetime",
        "numeric": "value_numeric"
    }

    value_field = field_map.get(value_type)
    if not value_field:
        return

    cursor.execute(f"""
        INSERT INTO obs (
            uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
            {value_field}, creator, date_created, voided
        ) VALUES (%s, %s, %s, %s, %s, 1, %s, 1, %s, 0)
    """, (obs_uuid, person_id, concept_id, encounter_id, now, value, now))

    obs_id = cursor.lastrowid
    cursor.execute("""
        INSERT INTO obs_migration_log (obs_id, person_id, encounter_id, concept_id, field_name, value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (obs_id, person_id, encounter_id, concept_id, field_name, str(value)))

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    interventiontype_map = load_value_map(cursor, "dreamsapp_interventiontype")

    cursor.execute("""
        SELECT m.* FROM tbl_behavioural_interven m
        INNER JOIN dreams_client_patient_mapping cp ON cp.client_id = m.client_id
        INNER JOIN Nuru_visits_mapping vp ON vp.patient_id = cp.patient_id
        WHERE m.client_id = 766959
    """)

    for row in cursor.fetchall():
        client_id = row["client_id"]
        patient_id = get_patient_id(cursor, int(client_id))

        if not patient_id:
            print(f"Skipping client_id {client_id} - missing patient")
            continue
        encounter_id = create_encounter(cursor, patient_id)
        for field, config in concept_map.items():
            value = row.get(field)
            if config["type"] == "coded" and field == "intervention_type_id":
                value = interventiontype_map.get(str(value))
            insert_obs(cursor, patient_id, encounter_id, config["concept_id"], value, config["type"], field)

    conn.commit()
    cursor.close()
    print("Behavioural data successfully migrated to obs with individual encounters.")

if __name__ == "__main__":
    main()
