import mysql.connector
import uuid
from datetime import datetime
from multiprocessing import Pool, cpu_count
import time
import random

# DB config
DEST_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password':'test',
    'database': 'openmrs'
}


concept_map = {
    "ever_tested_for_hiv_id": {"concept_id": 1000757, "type": "coded"},
    "period_last_tested_id": {"concept_id": 1000761, "type": "coded"},
    "last_test_result_id": {"concept_id": 1000763, "type": "coded"},
    "ccc_no": {"concept_id": 162053, "type": "text"},
    "enrolled_in_hiv_care_id": {"concept_id": 1000764, "type": "coded"},
    "care_facility_enrolled": {"concept_id": 1001719, "type": "text"},
    "reason_not_in_hiv_care_id": {"concept_id": 1000774, "type": "coded"},
    "reason_not_in_hiv_care_other": {"concept_id": 1000775, "type": "text"},
    "reasonnottestedforhiv_id": {"concept_id": 1000784, "type": "coded"},
    "reason_never_tested_for_hiv_other": {"concept_id": 1000785, "type": "text"},
    "knowledge_of_hiv_test_centres_id": {"concept_id": 1001720, "type": "coded"}
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
    conn = mysql.connector.connect(**DEST_DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    # load mappings
    categorical_map = load_value_map(cursor, "DreamsApp_categoricalresponse_mapping")
    period_last_test_map = load_value_map(cursor, "DreamsApp_periodresponse_mapping")
    hivtestresult_map = load_value_map(cursor, "DreamsApp_hivtestresultresponse_mapping")
    reasonnotinhivcare_map = load_value_map(cursor, "DreamsApp_reasonnotinhivcare_mapping")
    reasonnottestedforhiv_map = load_value_map(cursor, "DreamsApp_reasonnottestedforhiv_mapping")

    cursor.execute(""" SELECT *  FROM tbl_m_hivtesting hv
            WHERE EXISTS (
                SELECT 1 FROM dreams_client_patient_mapping pm
                WHERE pm.client_id = hv.client_id
            )
            AND EXISTS (
                SELECT 1 FROM tbl_m_demographics d
                WHERE d.client_id = hv.client_id AND d.implementing_partner_id =37)""")
    for row in cursor.fetchall():
        client_id = row["client_id"]       
        person_id, patient_id, encounter_id = get_person_and_encounter(cursor, int(client_id))
        if not person_id or not encounter_id:
            print(f"Skipping client_id {client_id} - missing person or encounter")
            continue
        for field, config in concept_map.items():
            value = row.get(field) 
            if config["type"] == "coded":
                if field in ("ever_tested_for_hiv_id", "enrolled_in_hiv_care_id", "knowledge_of_hiv_test_centres_id"):
                    value = categorical_map.get(str(value))
                elif field == "period_last_tested_id":
                    value = period_last_test_map.get(str(value))
                elif field == "last_test_result_id":
                    value = hivtestresult_map.get(str(value))
                elif field == "reason_not_in_hiv_care_id":
                    value = reasonnotinhivcare_map.get(str(value))
                elif field == "reasonnottestedforhiv_id":
                    value = reasonnottestedforhiv_map.get(str(value))
                if value is None:
                    continue   
            insert_obs(cursor, person_id, encounter_id, config["concept_id"], value, config["type"], field)
    conn.commit()
    cursor.close()
    print("HIV Testing Data successfully migrated to obs.")  
if __name__ == "__main__":
    main()
