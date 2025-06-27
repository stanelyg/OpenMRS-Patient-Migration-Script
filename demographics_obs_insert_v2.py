import mysql.connector
import uuid
from datetime import datetime
from multiprocessing import Pool, cpu_count
import time
import random

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

BATCH_SIZE = 2000  # reduce to avoid large locks
NUM_WORKERS = cpu_count()
MAX_RETRIES = 5
RETRY_BACKOFF = (2, 6)  # seconds

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

def process_batch(client_ids):
    for attempt in range(MAX_RETRIES):
        try:
            _run_batch_logic(client_ids)
            return
        except mysql.connector.Error as e:
            if e.errno == 1205:
                wait = random.uniform(*RETRY_BACKOFF)
                print(f"[Batch Retry {attempt+1}] Lock timeout. Retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                raise
    print(f"[Batch Failed] Gave up after {MAX_RETRIES} retries due to lock timeout.")

def _run_batch_logic(client_ids):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    value_maps = load_value_maps(cursor)

    format_strings = ','.join(['%s'] * len(client_ids))
    cursor.execute(f"""
        SELECT d.client_id, d.patient_id, p.encounter_id
        FROM dreams_client_patient_mapping d
        LEFT JOIN patient_encounter_mapping p ON d.patient_id = p.patient_id
        WHERE d.client_id IN ({format_strings})
    """, tuple(client_ids))
    client_map = {row['client_id']: (row['patient_id'], row['encounter_id']) for row in cursor.fetchall()}

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    obs_data = []
    log_data = []

    for client_id in client_ids:
        cursor.execute("SELECT * FROM tbl_m_demographics WHERE client_id = %s", (client_id,))
        row = cursor.fetchone()
        if not row or client_id not in client_map:
            continue

        person_id, encounter_id = client_map[client_id]
        if not person_id or not encounter_id:
            continue

        for field, config in concept_map.items():
            value = row.get(field)
            if value in (None, ""):
                continue

            if config["type"] == "coded" and field in value_maps:
                value = value_maps[field].get(str(value))
                if not value:
                    continue

            field_map = {
                "coded": "value_coded",
                "text": "value_text",
                "date": "value_datetime",
                "numeric": "value_numeric"
            }
            value_field = field_map.get(config["type"])
            if not value_field:
                continue

            obs_uuid = str(uuid.uuid4())
            obs_data.append((
                obs_uuid, person_id, config["concept_id"], encounter_id,
                now, 1, value, 1, now, 0
            ))

            log_data.append((None, person_id, encounter_id, config["concept_id"], field, str(value)))

    if obs_data:
        value_field = field_map[config["type"]]  # re-use last type seen
        insert_query = f"""
            INSERT INTO obs (
                uuid, person_id, concept_id, encounter_id, obs_datetime,
                location_id, {value_field}, creator, date_created, voided
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, obs_data)

        cursor.execute("SELECT LAST_INSERT_ID()")
        start_id = cursor.fetchone()['LAST_INSERT_ID()']

        for i in range(len(log_data)):
            log_data[i] = (start_id + i,) + log_data[i][1:]

        cursor.executemany("""
            INSERT INTO obs_migration_log 
            (obs_id, person_id, encounter_id, concept_id, field_name, value)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, log_data)

    conn.commit()
    cursor.close()
    conn.close()

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
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
