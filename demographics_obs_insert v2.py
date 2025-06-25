import mysql.connector
import uuid
from datetime import datetime
from multiprocessing import Pool, cpu_count

# Configuration
SOURCE_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'dreams_production'
}

DEST_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

BATCH_SIZE = 500  # Number of patients per process
NUM_WORKERS = cpu_count()

concept_map = {
    "implementing_partner_id": {"concept_id": 1001343, "type": "coded"},
    "date_of_enrollment": {"concept_id": 166091, "type": "date"},
    "verification_document_id": {"concept_id": 1000658, "type": "coded"},
    "verification_document_other": {"concept_id": 1000659, "type": "text"},
    "verification_doc_no": {"concept_id": 1000660, "type": "text"},
    "marital_status_id": {"concept_id": 1000636, "type": "coded"},
    "phone_number": {"concept_id": 159635, "type": "text"},
    "dss_id_number": {"concept_id": 1000661, "type": "text"},
    "county_of_residence_id": {"concept_id": 167131, "type": "coded"},
    "sub_county_id": {"concept_id": 1001016, "type": "coded"},
    "ward_id": {"concept_id": 1001021, "type": "coded"},
    "informal_settlement": {"concept_id": 1000662, "type": "text"},
    "village": {"concept_id": 1354, "type": "text"},
    "landmark": {"concept_id": 1000663, "type": "text"},
    "dreams_id": {"concept_id": 1000664, "type": "text"},
    "guardian_name": {"concept_id": 1000665, "type": "text"},
    "relationship_with_guardian": {"concept_id": 1001771, "type": "text"},
    "guardian_phone_number": {"concept_id": 1000667, "type": "text"},
    "guardian_national_id": {"concept_id": 1000668, "type": "text"},
    "external_organisation_id": {"concept_id": 1000669, "type": "coded"},
    "cpmis_id": {"concept_id": 1000670, "type": "text"},
    "nemis_no": {"concept_id": 1000671, "type": "text"},
    "nupi_no": {"concept_id": 1000672, "type": "text"}
}

def load_value_maps(cursor):
    def load_map(table):
        cursor.execute(f"SELECT id, concept_id FROM {table}")
        return {str(row[0]): row[1] for row in cursor.fetchall()}

    return {
        "implementing_partner_id": load_map("dreamsapp_implementingpartner"),
        "verification_document_id": load_map("DreamsApp_verificationdocument_mapping"),
        "marital_status_id": load_map("DreamsApp_maritalstatus_mapping"),
        "county_of_residence_id": load_map("dreamsapp_county"),
        "sub_county_id": load_map("dreamsapp_subcounty"),
        "ward_id": load_map("dreamsapp_ward"),
        "external_organisation_id": load_map("dreamsapp_externalorganisation")
    }

def get_person_and_encounter(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id = %s", (client_id,))
    row = cursor.fetchone()
    if not row:
        return None, None, None
    patient_id = row[0]
    cursor.execute("SELECT encounter_id FROM patient_encounter_mapping WHERE patient_id = %s", (patient_id,))
    encounter = cursor.fetchone()
    return patient_id, patient_id, encounter[0] if encounter else None

def insert_obs(cursor, person_id, encounter_id, concept_id, value, value_type, field_name):
    if value in (None, ""):
        return
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    uuid_str = str(uuid.uuid4())
    field_map = {
        "coded": "value_coded",
        "text": "value_text",
        "date": "value_datetime",
        "numeric": "value_numeric"
    }
    value_field = field_map.get(value_type)
    if not value_field:
        return

    # Insert into obs
    cursor.execute(f"""
        INSERT INTO obs (
            uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
            {value_field}, creator, date_created, voided
        )
        VALUES (%s, %s, %s, %s, %s, 1, %s, 1, %s, 0)
    """, (uuid_str, person_id, concept_id, encounter_id, now, value, now))

    obs_id = cursor.lastrowid

    # Log to obs_migration_log
    cursor.execute("""
        INSERT INTO obs_migration_log 
        (obs_id, person_id, encounter_id, concept_id, field_name, value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (obs_id, person_id, encounter_id, concept_id, field_name, str(value)))

def process_batch(client_ids):
    src_conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    dest_conn = mysql.connector.connect(**DEST_DB_CONFIG)
    src_cursor = src_conn.cursor(dictionary=True)
    dest_cursor = dest_conn.cursor()
    value_maps = load_value_maps(dest_cursor)

    for client_id in client_ids:
        src_cursor.execute("SELECT * FROM tbl_m_demographics WHERE client_id = %s", (client_id,))
        row = src_cursor.fetchone()
        if not row:
            continue

        person_id, _, encounter_id = get_person_and_encounter(dest_cursor, client_id)
        if not person_id or not encounter_id:
            continue

        for field, config in concept_map.items():
            value = row.get(field)
            if config["type"] == "coded" and field in value_maps:
                value = value_maps[field].get(str(value))
            insert_obs(dest_cursor, person_id, encounter_id, config["concept_id"], value, config["type"], field)

        dest_conn.commit()  # commit per patient

    src_cursor.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()

def main():
    conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT client_id FROM tbl_m_demographics WHERE client_id <= 2689322")
    client_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    batches = [client_ids[i:i + BATCH_SIZE] for i in range(0, len(client_ids), BATCH_SIZE)]

    with Pool(NUM_WORKERS) as pool:
        pool.map(process_batch, batches)

    print("All batches processed.")

if __name__ == "__main__":
    main()
