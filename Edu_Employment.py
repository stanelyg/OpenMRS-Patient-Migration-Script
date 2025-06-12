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
    "currently_in_school_id": {"concept_id": 1000711, "type": "coded"},
    "current_school_name": {"concept_id": 1000712, "type": "text"},
    "current_school_type_id": {"concept_id": 1000715, "type": "coded"},
    "current_school_level_id": {"concept_id": 1000720, "type": "coded"},
    "current_school_level_other": {"concept_id": 1001810, "type": "text"},
    "current_class": {"concept_id": 1000924, "type": "text"},
    "educationsupporter_id": {"concept_id": 1000743, "type": "coded"},
    "current_education_supporter_other": {"concept_id": 1001810, "type": "text"},
    "current_income_source_id": {"concept_id": 1000750, "type": "coded"},
    "current_income_source_other": {"concept_id": 1000751, "type": "text"},
    "has_savings_id": {"concept_id": 1000752, "type": "coded"},
    "banking_place_id": {"concept_id": 1000756, "type": "coded"},
    "banking_place_other": {"concept_id": 1001811, "type": "text"},
    "reason_not_in_school_id": {"concept_id": 1000728, "type": "coded"},
    "reason_not_in_school_other": {"concept_id": 1000729, "type": "text"},
    "last_time_in_school_id": {"concept_id": 1000735, "type": "coded"},
    "dropout_school_level_id": {"concept_id": 1000737, "type": "coded"},
    "dropout_class": {"concept_id": 1000738, "type": "text"},
    "life_wish_id": {"concept_id": 1000744, "type": "coded"},
    "life_wish_other": {"concept_id": 1000745, "type": "text"},
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
    current_school_type_map = load_value_map(cursor, "DreamsApp_schooltype_mapping")
    categorical_map = load_value_map(cursor, "dreams_categorical_responses")
    schoollevel_map = load_value_map(cursor, "DreamsApp_schoollevel_mapping")
    educationsupporter_map = load_value_map(cursor, "DreamsApp_educationsupporter_mapping")
    sourceofincome_map = load_value_map(cursor, "DreamsApp_sourceofincome_mapping")
    bankingplace_map = load_value_map(cursor, "DreamsApp_bankingplace_mapping")
    reasonnotinschool_map = load_value_map(cursor, "DreamsApp_reasonnotinschool_mapping")
    periodresponse_map = load_value_map(cursor, "DreamsApp_periodresponse_mapping")
    lifewish_map = load_value_map(cursor, "DreamsApp_lifewish_mapping")   

    df = pd.read_csv("Edu_Emp.csv")

    for _, row in df.iterrows():
        client_id = row["client_id"]
        person_id, patient_id, encounter_id = get_person_and_encounter(cursor, int(client_id))
        if not person_id or not encounter_id:
            print(f"Skipping client_id {client_id} - missing person or encounter")
            continue

        for field, config in concept_map.items():
            value = row.get(field)            
            if config["type"] == "coded":
                if field == "currently_in_school_id":
                    value = categorical_map.get(str(value))
                elif field == "current_school_type_id":
                    value=current_school_type_map.get(str(value))
                elif field == "current_school_level_id":
                    value=schoollevel_map.get(str(value))
                elif field == "educationsupporter_id":
                    value = educationsupporter_map.get(str(value))
                elif field == "current_income_source_id":
                    value = sourceofincome_map.get(str(value))
                elif field == "has_savings_id":
                    value = categorical_map.get(str(value))
                elif field == "banking_place_id":
                    value = bankingplace_map.get(str(value)) 
                elif field == "reason_not_in_school_id":
                    value = reasonnotinschool_map.get(str(value))
                elif field == "last_time_in_school_id":
                    value = periodresponse_map.get(str(value))
                elif field == "dropout_school_level_id":
                    value = schoollevel_map.get(str(value))
                elif field == "life_wish_id":
                    value = lifewish_map.get(str(value))               
            insert_obs(cursor, person_id[0], encounter_id, config["concept_id"], value, config["type"], field)
    conn.commit()
    conn.close()
    print("Data successfully migrated to obs.")

if __name__ == "__main__":
    main()
