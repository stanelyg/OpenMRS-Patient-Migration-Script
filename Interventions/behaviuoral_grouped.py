
import pandas as pd
import mysql.connector
import uuid
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
     'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

concept_map = {
    "intervention_type_id": {"concept_id": 1000880, "type": "coded"},
    "intervention_date": {"concept_id": 1000884, "type": "date"},
    "comment": {"concept_id": 1000653, "type": "text"},
    "other_specify": {"concept_id": 1001774, "type": "text"}
}

group_concept_id = 1001775  

def load_value_map(cursor, table_name):
    cursor.execute(f"SELECT id, concept_id FROM {table_name}")
    return {str(row['id']): row['concept_id'] for row in cursor.fetchall()}

def get_person_and_encounter(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id = %s", (client_id,))
    row = cursor.fetchone()
    if not row:
        return None, None, None
    patient_id = row['patient_id']
    cursor.execute("SELECT encounter_id FROM service_uptake_encounter_mapping WHERE patient_id = %s", (patient_id,))
    encounter_row = cursor.fetchone()
    if not encounter_row:
        return patient_id, patient_id, None
    return patient_id, patient_id, encounter_row['encounter_id']

def insert_obs_group(cursor, person_id, encounter_id, obs_datetime):
    obs_uuid = str(uuid.uuid4())
    cursor.execute("""
        INSERT INTO obs (uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
                         creator, date_created, voided)
        VALUES (%s, %s, %s, %s, %s, 1, 1, %s, 0)
    """, (obs_uuid, person_id, group_concept_id, encounter_id, obs_datetime, obs_datetime))
    return cursor.lastrowid

def insert_obs(cursor, person_id, encounter_id, concept_id, value, value_type, field_name, obs_group_id):
    if value is None or value == "":
        return
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    obs_uuid = str(uuid.uuid4())
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
        INSERT INTO obs (uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
                         {value_field}, creator, date_created, voided, obs_group_id)
        VALUES (%s, %s, %s, %s, %s, 1, %s, 1, %s, 0, %s)
    """, (obs_uuid, person_id, concept_id, encounter_id, now, value, now, obs_group_id))

    obs_id = cursor.lastrowid
    cursor.execute("""
        INSERT INTO services_obs_migration_log (obs_id, person_id, encounter_id, concept_id, field_name, value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (obs_id, person_id, encounter_id, concept_id, field_name, str(value)))

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    interventiontype_map = load_value_map(cursor, "dreamsapp_interventiontype")

    cursor.execute("""
        SELECT m.* FROM tbl_behavioural_interven m
        INNER JOIN dreams_client_patient_mapping cp ON cp.client_id = m.client_id
        INNER JOIN dreams_patient_visits_mapping vp ON vp.patient_id = cp.patient_id
    """)
    for row in cursor.fetchall():
        client_id = row["client_id"]
        person_id, patient_id, encounter_id = get_person_and_encounter(cursor, int(client_id))
        if not person_id or not encounter_id:
            print(f"Skipping client_id {client_id} due to missing patient or encounter")
            continue
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        obs_group_id = insert_obs_group(cursor, person_id, encounter_id, now)

        for field, config in concept_map.items():
            value = row.get(field)
            if config["type"] == "coded" and field == "intervention_type_id":
                value = interventiontype_map.get(str(value))
            insert_obs(cursor, person_id, encounter_id, config["concept_id"], value, config["type"], field, obs_group_id)

    conn.commit()
    cursor.close()
    print("behavioural data successfully migrated")

if __name__ == "__main__":
    main()
