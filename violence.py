import pandas as pd
import mysql.connector
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os
SOURCE_DB_CONFIG = {
    'host': 'localhost',
    'user':'root',
    'password': 'test',
    'database': 'dreams_production'
}

DEST_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}


concept_map = {
    "humiliated_ever_id": {"concept_id": 1000824, "type": "coded"},
    "humiliated_last_3months_id": {"concept_id": 1000825, "type": "coded"},
    "threats_to_hurt_ever_id": {"concept_id": 1000826, "type": "coded"},
    "threats_to_hurt_last_3months_id": {"concept_id": 1000827, "type": "coded"},
    "insulted_ever_id": {"concept_id": 1000828, "type": "coded"},
    "insulted_last_3months_id": {"concept_id": 1000832, "type": "coded"},
    "economic_threat_ever_id": {"concept_id": 1000833, "type": "coded"},
    "economic_threat_last_3months_id": {"concept_id": 1000834, "type": "coded"},
    "physical_violence_ever_id": {"concept_id": 1000835, "type": "coded"},
    "physical_violence_last_3months_id": {"concept_id": 1000836, "type": "coded"},
    "physically_forced_sex_ever_id": {"concept_id": 1000837, "type": "coded"},
    "physically_forced_sex_last_3months_id": {"concept_id": 1000838, "type": "coded"},
    "physically_forced_other_sex_acts_ever_id": {"concept_id": 1000839, "type": "coded"},
    "physically_forced_other_sex_acts_last_3months_id": {"concept_id": 1000840, "type": "coded"},
    "threatened_for_sexual_acts_ever_id": {"concept_id": 1000841, "type": "coded"},
    "threatened_for_sexual_acts_last_3months_id": {"concept_id": 1000842, "type": "coded"},
    "gbvhelpprovider_id": {"concept_id": 1000852, "type": "coded"},
    "gbv_help_provider_other": {"concept_id": 1001725, "type": "text"},
    "seek_help_after_gbv_id": {"concept_id": 1000843, "type": "coded"},
    "preferred_gbv_help_provider_id": {"concept_id": 1000854, "type": "coded"},
    "preferred_gbv_help_provider_other": {"concept_id": 1001726, "type": "text"}
}

# Load ID-to-concept mappings from lookup tables
def load_value_map(cursor, table_name):
    cursor.execute(f"SELECT id, concept_id FROM {table_name}")
    return {str(row['id']): row['concept_id'] for row in cursor.fetchall()}


def get_person_and_encounter(cursor,client_id):
    cursor.execute("""
        SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id = %s
    """, (client_id,))
    row = cursor.fetchone()
    if not row or 'patient_id' not in row:
        print(f"Missing patient_id for client_id {client_id}")
        return None, None, None

    patient_id = row['patient_id']

    cursor.execute("""
        SELECT encounter_id FROM patient_encounter_mapping WHERE patient_id = %s
    """, (patient_id,))
    encounter_row = cursor.fetchone()

    if not encounter_row or 'encounter_id' not in encounter_row:
        print(f"Missing encounter for patient_id {patient_id}")
        return patient_id, patient_id, None

    encounter_id = encounter_row['encounter_id']
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
    src_conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    dest_conn = mysql.connector.connect(**DEST_DB_CONFIG)
    src_cursor = src_conn.cursor(dictionary=True)
    dest_cursor = dest_conn.cursor(dictionary=True)
    
    categorical_map = load_value_map(dest_cursor, "DreamsApp_categoricalresponse_mapping")
    frequency_map = load_value_map(dest_cursor, "DreamsApp_frequencyresponse_mapping")
    gbvhelpprovider_map = load_value_map(dest_cursor, "DreamsApp_gbvhelpprovider_mapping")
 
    src_cursor.execute("SELECT * FROM tbl_m_violence")
    for row in src_cursor.fetchall():
        client_id = row["client_id"]
        person_id, patient_id, encounter_id = get_person_and_encounter(dest_cursor, int(client_id))
        if not person_id or not encounter_id:
            print(f"Skipping client_id {client_id} - missing person or encounter")
            continue

        for field, config in concept_map.items():
            value = row.get(field) 
            if config["type"] == "coded":
                if field == "humiliated_ever_id":
                    value = categorical_map.get(str(value))
                elif field == "humiliated_last_3months_id":
                    value = frequency_map.get(str(value))
                elif field == "threats_to_hurt_ever_id":
                    value=categorical_map.get(str(value))
                elif field == "threats_to_hurt_last_3months_id":
                    value=frequency_map.get(str(value))
                elif field == "insulted_ever_id":
                    value=categorical_map.get(str(value))
                elif field == "insulted_last_3months_id":
                    value=frequency_map.get(str(value))
                elif field == "economic_threat_ever_id":
                    value=categorical_map.get(str(value))
                elif field == "economic_threat_last_3months_id":
                    value=frequency_map.get(str(value))
                elif field == "physical_violence_ever_id":
                    value=categorical_map.get(str(value))
                elif field == "physical_violence_last_3months_id":
                    value=frequency_map.get(str(value)) 
                elif field == "physically_forced_sex_ever_id":
                    value=categorical_map.get(str(value))
                elif field == "physically_forced_sex_last_3months_id":
                    value = frequency_map.get(str(value))
                elif field == "physically_forced_other_sex_acts_ever_id":
                    value=categorical_map.get(str(value))
                elif field == "physically_forced_other_sex_acts_last_3months_id":
                    value = frequency_map.get(str(value))
                elif field == "threatened_for_sexual_acts_ever_id":
                    value=categorical_map.get(str(value))
                elif field == "threatened_for_sexual_acts_last_3months_id":
                    value = frequency_map.get(str(value))
                elif field == "gbvhelpprovider_id":
                    value=gbvhelpprovider_map.get(str(value))
                elif field == "preferred_gbv_help_provider_id":
                    value = gbvhelpprovider_map.get(str(value))


            insert_obs(dest_cursor, person_id[0], encounter_id, config["concept_id"],value, config["type"], field)
    dest_conn.commit()
    src_cursor.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()
    print("Violence data successfully migrated to obs.")

if __name__ == "__main__":
    main()
