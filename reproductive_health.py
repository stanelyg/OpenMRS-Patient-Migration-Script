import mysql.connector
import uuid
from datetime import datetime
from multiprocessing import Pool, cpu_count

# DB config
DEST_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'

}

concept_map = {
    "has_biological_children_id": {"concept_id": 1000806, "type": "coded"},
    "no_of_biological_children": {"concept_id": 1000709, "type": "numeric"},
    "currently_pregnant_id": {"concept_id": 1000807, "type": "coded"},
    "current_anc_enrollment_id": {"concept_id": 1001721, "type": "coded"},
    "anc_facility_name": {"concept_id": 1000809, "type": "text"},
    "fp_methods_awareness_id": {"concept_id": 1000810, "type": "coded"},
    "familyplanningmethod_id": {"concept_id": 1000817, "type": "coded"},
    "known_fp_method_other": {"concept_id": 1001722, "type": "text"},
    "currently_use_modern_fp_id": {"concept_id": 1000819, "type": "coded"},
    "current_fp_method_id": {"concept_id": 1000820, "type": "coded"},
    "current_fp_method_other": {"concept_id": 1001723, "type": "text"},
    "reason_not_using_fp_id": {"concept_id": 1000822, "type": "coded"},
    "reason_not_using_fp_other": {"concept_id": 1001724, "type": "text"}
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
    """, (obs_id, person_id, '', '', '', str(value)))

def main():
    conn = mysql.connector.connect(**DEST_DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    categorical_map = load_value_map(cursor, "DreamsApp_categoricalresponse_mapping")
    not_using_fp_map = load_value_map(cursor, "DreamsApp_reasonnotusingfamilyplanning_mapping")
    fp_method_map = load_value_map(cursor, "DreamsApp_familyplanningmethod_mapping")
    cursor.execute(""" SELECT * FROM tbl_m_reprohealth rp  WHERE EXISTS (
            SELECT 1 
            FROM dreams_client_patient_mapping pm 
            WHERE pm.client_id = rp.client_id
        )
        AND EXISTS (
            SELECT 1 
            FROM tbl_m_demographics d 
            WHERE d.client_id = rp.client_id AND d.implementing_partner_id =37
        )""")
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
                    "has_biological_children_id", "currently_pregnant_id", "current_anc_enrollment_id",
                    "fp_methods_awareness_id", "currently_use_modern_fp_id"
                ):
                    value = categorical_map.get(str(value))
                elif field in ("familyplanningmethod_id", "current_fp_method_id"):
                    value = fp_method_map.get(str(value))
                elif field == "reason_not_using_fp_id":
                    value = not_using_fp_map.get(str(value))
                if value is None:
                    continue
            insert_obs(cursor, person_id, encounter_id, config["concept_id"], value, config["type"], field)
    conn.commit()
    cursor.close()
    print("Reproductive health obs migration complete.")  
if __name__ == "__main__":
    main()

    


