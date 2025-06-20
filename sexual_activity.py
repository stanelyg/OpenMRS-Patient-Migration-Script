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
    "ever_had_sex_id": {"concept_id": 1000645, "type": "coded"},
    "age_at_first_sexual_encounter": {"concept_id": 1000787, "type": "numeric"},
    "has_sexual_partner_id": {"concept_id": 1000788, "type": "coded"},
    "sex_partners_in_last_12months": {"concept_id": 1000792, "type": "numeric"},
    "age_of_last_partner_id": {"concept_id": 1000792, "type": "coded"},
    "age_of_second_last_partner_id": {"concept_id": 1000793, "type": "coded"},
    "age_of_third_last_partner_id": {"concept_id": 1000794, "type": "coded"},
    "last_partner_circumcised_id": {"concept_id": 1000795, "type": "coded"},
    "second_last_partner_circumcised_id": {"concept_id": 1000796, "type": "coded"},
    "third_last_partner_circumcised_id": {"concept_id": 1000797, "type": "coded"},
    "know_last_partner_hiv_status_id": {"concept_id": 1000798, "type": "coded"},
    "know_second_last_partner_hiv_status_id": {"concept_id": 1000800, "type": "coded"},
    "know_third_last_partner_hiv_status_id": {"concept_id": 1000801, "type": "coded"},
    "used_condom_with_last_partner_id": {"concept_id": 1000802, "type": "coded"},
    "used_condom_with_second_last_partner_id": {"concept_id": 1000803, "type": "coded"},
    "used_condom_with_third_last_partner_id": {"concept_id": 1000804, "type": "coded"},
    "received_money_gift_for_sex_id": {"concept_id": 1000805, "type": "coded"}
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
    
    partner_age_map = load_value_map(dest_cursor, "DreamsApp_ageofsexualpartner_mapping")
    categorical_map = load_value_map(dest_cursor, "DreamsApp_categoricalresponse_mapping")
    frequency_map = load_value_map(dest_cursor, "DreamsApp_frequencyresponse_mapping")
 
    src_cursor.execute("SELECT * FROM tbl_m_sexualactivity")
    for row in src_cursor.fetchall():
        client_id = row["client_id"]
        person_id, patient_id, encounter_id = get_person_and_encounter(dest_cursor, int(client_id))
        if not person_id or not encounter_id:
            print(f"Skipping client_id {client_id} - missing person or encounter")
            continue

        for field, config in concept_map.items():
            value = row.get(field)  
            if config["type"] == "coded":
                if field == "ever_had_sex_id":
                    value = categorical_map.get(str(value))
                elif field == "has_sexual_partner_id":
                    value = categorical_map.get(str(value))
                elif field == "age_of_last_partner_id":
                    value=partner_age_map.get(str(value))
                elif field == "age_of_second_last_partner_id":
                    value=partner_age_map.get(str(value))
                elif field == "age_of_third_last_partner_id":
                    value=partner_age_map.get(str(value))
                elif field == "last_partner_circumcised_id":
                    value = categorical_map.get(str(value))
                elif field == "second_last_partner_circumcised_id":
                    value = categorical_map.get(str(value))
                elif field == "third_last_partner_circumcised_id":
                    value = categorical_map.get(str(value))
                elif field == "know_last_partner_hiv_status_id":
                    value = categorical_map.get(str(value))
                elif field == "know_second_last_partner_hiv_status_id":
                    value = categorical_map.get(str(value))
                elif field == "know_third_last_partner_hiv_status_id":
                    value = categorical_map.get(str(value))
                elif field == "received_money_gift_for_sex_id":
                    value = categorical_map.get(str(value))              
                elif field == "used_condom_with_last_partner_id":
                    value=frequency_map.get(str(value))
                elif field == "used_condom_with_second_last_partner_id":
                    value = frequency_map.get(str(value))
                elif field == "used_condom_with_third_last_partner_id":
                    value = frequency_map.get(str(value))

            insert_obs(dest_cursor, person_id, encounter_id, config["concept_id"],value, config["type"], field)
    dest_conn.commit()
    src_cursor.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()
    print("Data successfully migrated sexual activity data to obs.")

if __name__ == "__main__":
    main()
