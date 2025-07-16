import mysql.connector
import uuid
from datetime import datetime
from multiprocessing import Pool, cpu_count
import time
import random

# DB config
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

BATCH_SIZE = 1000
NUM_WORKERS = cpu_count()
MAX_RETRIES = 5
RETRY_BACKOFF = (2, 6)

concept_map = {
    "head_of_household_id": {"concept_id": 1000686, "type": "coded"},
    "head_of_household_other": {"concept_id": 1001707, "type": "text"},
    "age_of_household_head": {"concept_id": 1000673, "type": "numeric"},
    "is_father_alive": {"concept_id": 1000674, "type": "coded"},
    "is_mother_alive": {"concept_id": 1000675, "type": "coded"},
    "is_parent_chronically_ill": {"concept_id": 1000677, "type": "coded"},
    "main_floor_material_id": {"concept_id": 1000679, "type": "coded"},
    "main_floor_material_other": {"concept_id": 1000680, "type": "text"},
    "main_roof_material_id": {"concept_id": 1000685, "type": "coded"},
    "main_roof_material_other": {"concept_id": 1001708, "type": "text"},
    "main_wall_material_id": {"concept_id": 1000691, "type": "coded"},
    "main_wall_material_other": {"concept_id": 1000692, "type": "text"},
    "source_of_drinking_water_id": {"concept_id": 1000698, "type": "coded"},
    "source_of_drinking_water_other": {"concept_id": 1000699, "type": "text"},
    "no_of_days_missed_food_in_4wks_id": {"concept_id": 1000700, "type": "coded"},
    "has_disability_id": {"concept_id": 164951, "type": "coded"},
    "disabilitytype_id": {"concept_id": 1001091, "type": "coded"},
    "disability_type_other": {"concept_id": 1001709, "type": "text"},
    "no_of_people_in_household": {"concept_id": 1000705, "type": "numeric"},
    "no_of_females": {"concept_id": 1000706, "type": "numeric"},
    "no_of_males": {"concept_id": 1000707, "type": "numeric"},
    "no_of_children": {"concept_id": 1000709, "type": "numeric"},
    "ever_enrolled_in_ct_program_id": {"concept_id": 1000710, "type": "coded"},
    "currently_in_ct_program_id": {"concept_id": 1001770, "type": "coded"},
    "current_ct_program": {"concept_id": 1001711, "type": "text"}
}

def load_value_map(cursor, table_name):
    cursor.execute(f"SELECT id, concept_id FROM {table_name}")
    result = cursor.fetchall()
    return {str(row['id']): row['concept_id'] for row in result} if result else {}

def load_all_maps(cursor):
    return {
        "head_of_household_id": load_value_map(cursor, "DreamsApp_householdhead_mapping"),
        "categorical": load_value_map(cursor, "DreamsApp_categoricalresponse_mapping"),
        "floor": load_value_map(cursor, "dreamsapp_floormaterial"),
        "roof": load_value_map(cursor, "dreamsapp_roofingmaterial"),
        "wall": load_value_map(cursor, "dreamsapp_wallmaterial"),
        "water": load_value_map(cursor, "dreamsapp_drinkingwater"),
        "disability": load_value_map(cursor, "dreamsapp_disabilitytype")
    }

def get_person_and_encounter(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id = %s", (client_id,))
    row = cursor.fetchone()
    if cursor.with_rows:
        cursor.fetchall()
    if not row:
        return None, None, None
    patient_id = row['patient_id']

    cursor.execute("SELECT encounter_id FROM patient_encounter_mapping WHERE patient_id = %s", (patient_id,))
    encounter = cursor.fetchone()
    if cursor.with_rows:
        cursor.fetchall()
    return patient_id, patient_id, encounter['encounter_id'] if encounter else None

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

def _run_batch_logic(client_ids):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    maps = load_all_maps(cursor)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    temp_data = []

    for client_id in client_ids:
        cursor.execute("SELECT * FROM tbl_m_household WHERE client_id = %s", (client_id,))
        row = cursor.fetchone()
        if cursor.with_rows:
            cursor.fetchall()
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
                if field == "head_of_household_id":
                    value = maps["head_of_household_id"].get(str(value))
                elif field in (
                    "is_father_alive", "is_mother_alive", "is_parent_chronically_ill",
                    "no_of_days_missed_food_in_4wks_id", "has_disability_id",
                    "ever_enrolled_in_ct_program_id", "currently_in_ct_program_id"
                ):
                    value = maps["categorical"].get(str(value))
                elif field == "main_floor_material_id":
                    value = maps["floor"].get(str(value))
                elif field == "main_roof_material_id":
                    value = maps["roof"].get(str(value))
                elif field == "main_wall_material_id":
                    value = maps["wall"].get(str(value))
                elif field == "source_of_drinking_water_id":
                    value = maps["water"].get(str(value))
                elif field == "disabilitytype_id":
                    value = maps["disability"].get(str(value))

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
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT h.client_id FROM tbl_m_household h
        INNER JOIN dreams_client_patient_mapping pm ON h.client_id = pm.client_id
        INNER JOIN tbl_m_demographics d ON h.client_id = d.client_id
        WHERE d.implementing_partner_id IN (35, 37, 39)
    """)
    client_ids = [row['client_id'] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    batches = [client_ids[i:i + BATCH_SIZE] for i in range(0, len(client_ids), BATCH_SIZE)]

    with Pool(NUM_WORKERS) as pool:
        pool.map(process_batch, batches)

    print("Household obs migration complete.")

if __name__ == "__main__":
    main()
