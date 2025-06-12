import pandas as pd
import mysql.connector
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os
DB_CONFIG = {
    'host': 'localhost',
    'user': 'henryg',
    'password': 'P@ssw0rd@1234',
    'database': 'openmrs'
}


concept_map = {
    "has_biological_children_id": {"concept_id": 1000806, "type": "coded"},
    "no_of_biological_children": {"concept_id": 1000709, "type": "numeric"},
    "currently_pregnant_id": {"concept_id": 1000807, "type": "coded"},
    "current_anc_enrollment_id": {"concept_id": 1000808, "type": "coded"},
    "anc_facility_name": {"concept_id": 1000809, "type": "text"},
    "fp_methods_awareness_id": {"concept_id": 1000810, "type": "coded"},
    "familyplanningmethod_id": {"concept_id": 1000817, "type": "coded"},
    "known_fp_method_other": {"concept_id": 1001280, "type": "text"},
    "currently_use_modern_fp_id": {"concept_id": 1000819, "type": "coded"},
    "current_fp_method_id": {"concept_id": 1000820, "type": "coded"},
    "current_fp_method_other": {"concept_id": 1001280, "type": "text"},
    "reason_not_using_fp_id": {"concept_id": 1000822, "type": "coded"},
    "reason_not_using_fp_other": {"concept_id": 1001280, "type": "text"},
}


# Load ID-to-concept mappings from lookup tables
def load_value_map(cursor, table_name):
    cursor.execute(f"SELECT id, concept_id FROM {table_name}")
    return {str(row[0]): row[1] for row in cursor.fetchall()}


def get_person_and_encounter(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id = %s", (client_id,))
    row = cursor.fetchone()
    if not row:
        return None, None, None
    patient_id = row
    cursor.execute("SELECT encounter_id FROM patient_encounter_mapping WHERE patient_id = %s", patient_id)
    encounter_row = cursor.fetchone()
    encounter_id = encounter_row[0] if encounter_row else None
    return patient_id, patient_id, encounter_id

def cast_to_number(value):
    try:
        num = float(value)
        if num.is_integer():
            return int(num)
        return num
    except (ValueError, TypeError):
        return value
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
        )
        VALUES (%s, %s, %s, %s, %s, 1, %s, 1, %s, 0)
    """, (obs_uuid, person_id, concept_id, encounter_id, now, value, now))

    obs_id = cursor.lastrowid

    cursor.execute("""
        INSERT INTO obs_migration_log (obs_id, person_id, encounter_id, concept_id, field_name, value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (obs_id, person_id, encounter_id, concept_id, field_name, str(value)))


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    categorical_map = load_value_map(cursor, "dreams_categorical_responses")
    not_using_fp_map = load_value_map(cursor, "DreamsApp_reasonnotusingfamilyplanning_mapping")
    fp_method_map = load_value_map(cursor, "DreamsApp_familyplanningmethod_mapping")
 
    df = pd.read_csv("csvs/reproductive_health.csv")

    for _, row in df.iterrows():
        client_id = row["client_id"]
        person_id, patient_id, encounter_id = get_person_and_encounter(cursor, int(client_id))
        if not person_id or not encounter_id:
            print(f"Skipping client_id {client_id} - missing person or encounter")
            continue

        for field, config in concept_map.items():
            value = row.get(field)
            if config["type"] == "coded":
                if field == "has_biological_children_id":
                    value = categorical_map.get(str(value))
                elif field == "currently_pregnant_id":
                    value = categorical_map.get(str(value))
                elif field == "current_anc_enrollment_id":
                    value=categorical_map.get(str(value))
                elif field == "fp_methods_awareness_id":
                    value=categorical_map.get(str(value))
                elif field == "familyplanningmethod_id":
                    value=fp_method_map.get(str(value))
                elif field == "currently_use_modern_fp_id":
                    value = categorical_map.get(str(value))
                elif field == "current_fp_method_id":
                    value=fp_method_map.get(str(value))
                elif field == "reason_not_using_fp_id":
                    value = not_using_fp_map.get(str(value))

            insert_obs(cursor, person_id[0], encounter_id, config["concept_id"],cast_to_number(value), config["type"], field)
    conn.commit()
    conn.close()
    print("Data successfully migrated to obs.")

if __name__ == "__main__":
    main()
