import mysql.connector
import uuid
from datetime import datetime
from multiprocessing import Pool, cpu_count

# DB config
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

BATCH_SIZE = 100000
NUM_WORKERS = cpu_count()

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
    return {str(row[0]): row[1] for row in cursor.fetchall()}

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
    obs_uuid = str(uuid.uuid4())

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

def process_batch(client_ids):
    src_conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    dest_conn = mysql.connector.connect(**DEST_DB_CONFIG)
    src_cursor = src_conn.cursor(dictionary=True)
    dest_cursor = dest_conn.cursor()
    maps = load_all_maps(dest_cursor)

    for client_id in client_ids:
        src_cursor.execute("SELECT * FROM tbl_m_household WHERE client_id = %s", (client_id,))
        row = src_cursor.fetchone()
        if not row:
            continue
        person_id, _, encounter_id = get_person_and_encounter(dest_cursor, client_id)
        if not person_id or not encounter_id:
            continue

        for field, config in concept_map.items():
            value = row.get(field)
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

            insert_obs(dest_cursor, person_id, encounter_id, config["concept_id"], value, config["type"], field)

        dest_conn.commit()

    src_cursor.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()

def main():
    conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT client_id FROM tbl_m_household WHERE client_id <= 2689322")
    client_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    batches = [client_ids[i:i + BATCH_SIZE] for i in range(0, len(client_ids), BATCH_SIZE)]

    with Pool(NUM_WORKERS) as pool:
        pool.map(process_batch, batches)

    print("Household obs migration complete.")

if __name__ == "__main__":
    main()
