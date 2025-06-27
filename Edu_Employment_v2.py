import mysql.connector
import uuid
from datetime import datetime
from multiprocessing import Pool, cpu_count

# DB configs
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

BATCH_SIZE = 1000
NUM_WORKERS = cpu_count()

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
    return {str(row[0]): row[1] for row in cursor.fetchall()}

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
    cursor.execute(f"""
        INSERT INTO obs (
            uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
            {value_field}, creator, date_created, voided
        )
        VALUES (%s, %s, %s, %s, %s, 1, %s, 1, %s, 0)
    """, (uuid_str, person_id, concept_id, encounter_id, now, value, now))
    obs_id = cursor.lastrowid
    cursor.execute("""
        INSERT INTO obs_migration_log (
            obs_id, person_id, encounter_id, concept_id, field_name, value
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (obs_id, person_id, encounter_id, concept_id, field_name, str(value)))

def process_batch(client_ids):
    src_conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    dest_conn = mysql.connector.connect(**DEST_DB_CONFIG)
    src_cursor = src_conn.cursor(dictionary=True)
    dest_cursor = dest_conn.cursor()
    mappings = load_all_mappings(dest_cursor)

    for client_id in client_ids:
        src_cursor.execute("SELECT * FROM tbl_m_edu_empl WHERE client_id = %s", (client_id,))
        row = src_cursor.fetchone()
        if not row:
            continue
        person_id, _, encounter_id = get_person_and_encounter(dest_cursor, client_id)
        if not person_id or not encounter_id:
            continue

        for field, config in concept_map.items():
            value = row.get(field)
            if config["type"] == "coded":
                if field == "currently_in_school_id" or field == "has_savings_id":
                    value = mappings["categorical"].get(str(value))
                elif field == "current_school_type_id":
                    value = mappings["school_type"].get(str(value))
                elif field == "current_school_level_id" or field == "dropout_school_level_id":
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
            insert_obs(dest_cursor, person_id, encounter_id, config["concept_id"], value, config["type"], field)
        dest_conn.commit()

    src_cursor.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()

def main():
    conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT client_id FROM tbl_m_edu_empl WHERE client_id <= 2689322")
    client_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    batches = [client_ids[i:i + BATCH_SIZE] for i in range(0, len(client_ids), BATCH_SIZE)]

    with Pool(NUM_WORKERS) as pool:
        pool.map(process_batch, batches)

    print("Education/employment obs migration complete.")

if __name__ == "__main__":
    main()
