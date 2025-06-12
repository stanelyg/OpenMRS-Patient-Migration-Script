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
    "gbv_help_provider_other": {"concept_id": 1001280, "type": "text"},
    "seek_help_after_gbv_id": {"concept_id": 1000853, "type": "coded"},
    "preferred_gbv_help_provider_id": {"concept_id": 1000854, "type": "coded"},
    "preferred_gbv_help_provider_other": {"concept_id": 1001816, "type": "text"}
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
    frequency_map = load_value_map(cursor, "DreamsApp_frequencyresponse_mapping")
    gbvhelpprovider_map = load_value_map(cursor, "DreamsApp_gbvhelpprovider_mapping")
 
    df = pd.read_csv("csvs/violence.csv")

    for _, row in df.iterrows():
        client_id = row["client_id"]
        person_id, patient_id, encounter_id = get_person_and_encounter(cursor, int(client_id))
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


            insert_obs(cursor, person_id[0], encounter_id, config["concept_id"],cast_to_number(value), config["type"], field)
    conn.commit()
    conn.close()
    print("Data successfully migrated to obs.")

if __name__ == "__main__":
    main()
