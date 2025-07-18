import mysql.connector
import uuid
from datetime import datetime
from multiprocessing import Pool, cpu_count
import time
import random

# DB config
DB_CONFIG = {
    'host': 'localhost',
    'user': 'henryg',
    'password': 'P@ssw0rd@1234',
    'database': 'openmrs'
}

BATCH_SIZE = 500
NUM_WORKERS = cpu_count()
MAX_RETRIES = 5
RETRY_BACKOFF = (2, 6)

concept_map = {
    "currently_in_school_id": {"concept_id": 1000711, "type": "coded"},
    "current_school_name": {"concept_id": 1000712, "type": "text"},
    "current_school_type_id": {"concept_id": 1000715, "type": "coded"},
    "current_school_level_id": {"concept_id": 1000720, "type": "coded"},
    "current_school_level_other": {"concept_id": 1001712, "type": "text"},
    "current_class": {"concept_id": 1001713, "type": "text"},
    "educationsupporter_id": {"concept_id": 1000743, "type": "coded"},
    "current_education_supporter_other": {"concept_id": 1001714, "type": "text"},
    "current_income_source_id": {"concept_id": 1000750, "type": "coded"},
    "current_income_source_other": {"concept_id": 1001715, "type": "text"},
    "has_savings_id": {"concept_id": 1000752, "type": "coded"},
    "banking_place_id": {"concept_id": 1000756, "type": "coded"},
    "banking_place_other": {"concept_id": 1001716, "type": "text"},
    "reason_not_in_school_id": {"concept_id": 1000927, "type": "coded"},
    "reason_not_in_school_other": {"concept_id": 1001717, "type": "text"},
    "last_time_in_school_id": {"concept_id": 1000735, "type": "coded"},
    "dropout_school_level_id": {"concept_id": 1000737, "type": "coded"},
    "dropout_class": {"concept_id": 1001718, "type": "text"},
    "life_wish_id": {"concept_id": 1000744, "type": "coded"},
    "life_wish_other": {"concept_id": 1000745, "type": "text"},
}

def load_value_map(cursor, table_name):
    cursor.execute(f"SELECT id, concept_id FROM {table_name}")
    return {str(row['id']): row['concept_id'] for row in cursor.fetchall()}

def load_all_mappings(cursor):
    return {
        "categorical": load_value_map(cursor, "DreamsApp_categoricalresponse_mapping"),
        "school_type": load_value_map(cursor, "DreamsApp_schooltype_mapping"),
        "school_level": load_value_map(cursor, "DreamsApp_schoollevel_mapping"),
        "supporter": load_value_map(cursor, "DreamsApp_educationsupporter_mapping"),
        "income": load_value_map(cursor, "DreamsApp_sourceofincome_mapping"),
        "banking": load_value_map(cursor, "DreamsApp_bankingplace_mapping"),
        "reason_not_in_school": load_value_map(cursor, "DreamsApp_reasonnotinschool_mapping"),
        "last_time_in_school": load_value_map(cursor, "DreamsApp_periodresponse_mapping"),
        "life_wish": load_value_map(cursor, "DreamsApp_lifewish_mapping")
    }

def get_person_and_encounter(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id = %s", (client_id,))
    row = cursor.fetchone()
    if not row:
        return None, None, None
    patient_id = row['patient_id']
    cursor.execute("SELECT encounter_id FROM patient_encounter_mapping WHERE patient_id = %s", (patient_id,))
    encounter = cursor.fetchone()
    return patient_id, patient_id, encounter['encounter_id'] if encounter else None

def process_batch(client_ids):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    mappings = load_all_mappings(cursor)

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    temp_data = []

    for client_id in client_ids:
        cursor.execute("SELECT * FROM tbl_m_edu_empl WHERE client_id = %s", (client_id,))
        row = cursor.fetchone()
        if not row:
            continue
        person_id, _, encounter_id = get_person_and_encounter(cursor, client_id)
        if not person_id or not encounter_id:
            continue

        for field, config in concept_map.items():
            value = row.get(field)
            if value in (None, ""):
                continue

            if config["type"] == "coded":
                if field in ("currently_in_school_id", "has_savings_id"):
                    value = mappings["categorical"].get(str(value))
                elif field == "current_school_type_id":
                    value = mappings["school_type"].get(str(value))
                elif field in ("current_school_level_id", "dropout_school_level_id"):
                    value = mappings["school_level"].get(str(value))
                elif field == "educationsupporter_id":
                    value = mappings["supporter"].get(str(value))
                elif field == "current_income_source_id":
                    value = mappings["income"].get(str(value))
                elif field == "banking_place_id":
                    value = mappings["banking"].get(str(value))
                elif field == "reason_not_in_school_id":
                    value = mappings["reason_not_in_school"].get(str(value))
                elif field == "last_time_in_school_id":
                    value = mappings["last_time_in_school"].get(str(value))
                elif field == "life_wish_id":
                    value = mappings["life_wish"].get(str(value))

                if value is None:
                    continue

            type_map = {
                "coded": "value_coded",
                "text": "value_text",
                "date": "value_datetime",
                "numeric": "value_numeric"
            }
            value_type = type_map[config["type"]]

            obs_uuid = str(uuid.uuid4())
            temp_data.append((
                obs_uuid, person_id, config["concept_id"], encounter_id,
                now, 1, value_type,
                value if value_type == "value_text" else None,
                value if value_type == "value_coded" else None,
                value if value_type == "value_datetime" else None,
                value if value_type == "value_numeric" else None,
                1, now, 0
            ))

    if temp_data:
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
                uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
                value_type, value_text, value_coded, value_datetime, value_numeric,
                creator, date_created, voided
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, temp_data)

        cursor.execute("CALL insert_from_temp_obs()")

    conn.commit()
    cursor.close()
    conn.close()

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(""" SELECT ed.client_id  FROM tbl_m_edu_empl ed
                        WHERE EXISTS (
                            SELECT 1 FROM tbl_m_demographics d
                            WHERE d.client_id = ed.client_id AND d.implementing_partner_id = 37
                        )
                        AND EXISTS (
                            SELECT 1 FROM dreams_client_patient_mapping pm
                            WHERE pm.client_id = ed.client_id) """)
    client_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    batches = [client_ids[i:i + BATCH_SIZE] for i in range(0, len(client_ids), BATCH_SIZE)]

    with Pool(NUM_WORKERS) as pool:
        pool.map(process_batch, batches)

    print("Education/employment obs migration complete.")

if __name__ == "__main__":
    main()
