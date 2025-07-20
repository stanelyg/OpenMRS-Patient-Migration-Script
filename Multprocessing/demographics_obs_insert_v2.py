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

BATCH_SIZE = 100
NUM_WORKERS = cpu_count()
MAX_RETRIES = 5
RETRY_BACKOFF = (2, 6)

LOG_EXTRA_FIELDS = True  # Toggle this to log more than obs_id

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

def load_lookup_map(cursor, table):
    cursor.execute(f"SELECT id, concept_id FROM {table}")
    return {str(row['id']): row['concept_id'] for row in cursor.fetchall()}

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

    ip_map = load_lookup_map(cursor, "dreamsapp_implementingpartner")
    doc_ver_map = load_lookup_map(cursor, "DreamsApp_verificationdocument_mapping")
    marital_status_map = load_lookup_map(cursor, "DreamsApp_maritalstatus_mapping")
    county_map = load_lookup_map(cursor, "dreamsapp_county")
    subcounty_map = load_lookup_map(cursor, "dreamsapp_subcounty")
    ward_map = load_lookup_map(cursor, "dreamsapp_ward")
    ext_org_map = load_lookup_map(cursor, "dreamsapp_externalorganisation")

    format_strings = ','.join(['%s'] * len(client_ids))
    cursor.execute(f"""
        SELECT d.client_id, d.patient_id, p.encounter_id
        FROM dreams_client_patient_mapping d
        LEFT JOIN patient_encounter_mapping p ON d.patient_id = p.patient_id
        WHERE d.client_id IN ({format_strings})
    """, tuple(client_ids))
    client_map = {row['client_id']: (row['patient_id'], row['encounter_id']) for row in cursor.fetchall()}

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    temp_data = []

    type_map = {
        "coded": "value_coded",
        "text": "value_text",
        "date": "value_datetime",
        "numeric": "value_numeric"
    }

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

            if config["type"] == "coded":
                if field == "implementing_partner_id":
                    value = ip_map.get(str(value))
                elif field == "verification_document_id":
                    value = doc_ver_map.get(str(value))
                elif field == "marital_status_id":
                    value = marital_status_map.get(str(value))
                elif field == "county_of_residence_id":
                    value = county_map.get(str(value))
                elif field == "sub_county_id":
                    value = subcounty_map.get(str(value))
                elif field == "ward_id":
                    value = ward_map.get(str(value))
                elif field == "external_organisation_id":
                    value = ext_org_map.get(str(value))
                if value is None:
                    continue

            value_type = type_map[config["type"]]
            obs_uuid = str(uuid.uuid4())
            temp_row = (
                obs_uuid, person_id, config["concept_id"], encounter_id,
                now, 1, value_type,
                value if value_type == "value_text" else None,
                value if value_type == "value_coded" else None,
                value if value_type == "value_datetime" else None,
                value if value_type == "value_numeric" else None,
                1, now, 0
            )
            temp_data.append(temp_row)

    if not temp_data:
        cursor.close()
        conn.close()
        return

    cursor.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS temp_obs (
            uuid CHAR(38),
            person_id INT,
            concept_id INT,
            encounter_id INT,
            obs_datetime DATETIME,
            location_id INT,
            value_type ENUM('value_coded', 'value_text', 'value_datetime', 'value_numeric'),
            value_text TEXT,
            value_coded INT,
            value_datetime DATETIME,
            value_numeric DECIMAL(10,2),
            creator INT,
            date_created DATETIME,
            voided TINYINT
        ) ENGINE=InnoDB
    """)

    cursor.executemany("""
        INSERT INTO temp_obs (
            uuid, person_id, concept_id, encounter_id, obs_datetime,
            location_id, value_type, value_text, value_coded,
            value_datetime, value_numeric, creator, date_created, voided
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, temp_data)

    cursor.callproc("insert_from_temp_obs")

    cursor.execute("SELECT LAST_INSERT_ID()")
    last_id = cursor.fetchone()['LAST_INSERT_ID()']
    total_inserted = len(temp_data)
    obs_ids = [(last_id + i,) for i in range(total_inserted)]

    if LOG_EXTRA_FIELDS:
        cursor.executemany("INSERT INTO obs_migration_log (obs_id) VALUES (%s)", obs_ids)

    conn.commit()
    cursor.close()
    conn.close()

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""SELECT d.client_id
        FROM tbl_m_demographics d
        INNER JOIN dreams_client_patient_mapping pm ON d.client_id = pm.client_id
        WHERE d.implementing_partner_id = 37""") #
    client_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    batches = [client_ids[i:i + BATCH_SIZE] for i in range(0, len(client_ids), BATCH_SIZE)]

    with Pool(NUM_WORKERS) as pool:
        pool.map(process_batch, batches)

    print("All batches processed.")

if __name__ == "__main__":
    main()
