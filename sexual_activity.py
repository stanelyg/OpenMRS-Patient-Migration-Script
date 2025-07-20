import pandas as pd
import mysql.connector
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os

DB_CONFIG = {
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
    cursor = conn.cursor(dictionary=True)
   
    categorical_map = load_value_map(cursor, "DreamsApp_categoricalresponse_mapping")
    partner_age_map = load_value_map(cursor, "DreamsApp_ageofsexualpartner_mapping")
    frequency_map = load_value_map(cursor, "DreamsApp_frequencyresponse_mapping")
 
    cursor.execute("""SELECT *  FROM tbl_m_sexualactivity sxa
            WHERE EXISTS (
                SELECT 1 FROM dreams_client_patient_mapping pm
                WHERE pm.client_id = sxa.client_id
            )
            AND EXISTS (
                SELECT 1 FROM tbl_m_demographics d
                WHERE d.client_id = sxa.client_id AND d.implementing_partner_id =37) 
    """)
    for row in cursor.fetchall():
        client_id = row["client_id"]
        person_id, patient_id, encounter_id = get_person_and_encounter(cursor, int(client_id))
        if not person_id or not encounter_id:
            print(f"Skipping client_id {client_id} - missing person or encounter")
            continue

        for field, config in concept_map.items():
            value = row.get(field) 
            if config["type"] == "coded":
                if field in (
                    "ever_had_sex_id", "has_sexual_partner_id",
                    "last_partner_circumcised_id", "second_last_partner_circumcised_id",
                    "third_last_partner_circumcised_id", "know_last_partner_hiv_status_id",
                    "know_second_last_partner_hiv_status_id", "know_third_last_partner_hiv_status_id",
                    "received_money_gift_for_sex_id"):
                    value = categorical_map.get(str(value))
                elif field in (
                    "age_of_last_partner_id", "age_of_second_last_partner_id", "age_of_third_last_partner_id"):
                    value = partner_age_map.get(str(value))
                elif field in (
                    "used_condom_with_last_partner_id", "used_condom_with_second_last_partner_id",
                    "used_condom_with_third_last_partner_id"):
                    value = frequency_map.get(str(value))
                if value is None:
                    continue


            insert_obs(cursor, person_id, encounter_id, config["concept_id"],value, config["type"], field)
    conn.commit()
    cursor.close()
    print("Sexual Activityv data successfully migrated to obs.")

if __name__ == "__main__":
    main()
