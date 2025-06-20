import pandas as pd
import mysql.connector
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os

SOURCE_DB_CONFIG = {
    'host': 'localhost',
    'user':'root',
    'password': 'test',
    'database': 'dreams_production'
}

DEST_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}


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
    "current_ct_program": {"concept_id": 1001711, "type": "text"},
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
    print(person_id,'-',encounter_id)
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
    src_conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    dest_conn = mysql.connector.connect(**DEST_DB_CONFIG)
    src_cursor = src_conn.cursor(dictionary=True)
    dest_cursor = dest_conn.cursor(dictionary=True)

    householdhead_map = load_value_map(dest_cursor, "DreamsApp_householdhead_mapping")
    categorical_map = load_value_map(dest_cursor, "DreamsApp_categoricalresponse_mapping")
    floormaterial_map = load_value_map(dest_cursor, "dreamsapp_floormaterial")
    roofingmaterial_map = load_value_map(dest_cursor, "dreamsapp_roofingmaterial")
    wallmaterial_map = load_value_map(dest_cursor, "dreamsapp_wallmaterial")
    drinkingwater_map = load_value_map(dest_cursor, "dreamsapp_drinkingwater")
    disabilitytype_map = load_value_map(dest_cursor, "dreamsapp_disabilitytype")
    

    # Read source data
    src_cursor.execute("SELECT * FROM tbl_m_household")
    for row in src_cursor.fetchall():
        client_id = row["client_id"]
        person_id, patient_id, encounter_id = get_person_and_encounter(dest_cursor, int(client_id))
        if not person_id or not encounter_id:
            print(f"Skipping client_id {client_id} - missing person or encounter")
            continue

        for field, config in concept_map.items():
            value = row.get(field)          
            if config["type"] == "coded":
                if field == "head_of_household_id":
                    value = householdhead_map.get(str(value))
                elif field == "is_father_alive":
                    value=categorical_map.get(str(value))
                elif field == "is_mother_alive":
                    value=categorical_map.get(str(value))
                elif field == "is_parent_chronically_ill":
                    value = categorical_map.get(str(value))
                elif field == "main_floor_material_id":
                    value = floormaterial_map.get(str(value))
                elif field == "main_roof_material_id":
                    value = roofingmaterial_map.get(str(value))
                elif field == "main_wall_material_id":
                    value = wallmaterial_map.get(str(value))  
                elif field == "source_of_drinking_water_id":
                    value = drinkingwater_map.get(str(value))
                elif field == "no_of_days_missed_food_in_4wks_id":
                    value = categorical_map.get(str(value))
                elif field == "has_disability_id":
                    value = categorical_map.get(str(value))
                elif field == "disabilitytype_id":
                    value = disabilitytype_map.get(str(value))
                elif field == "ever_enrolled_in_ct_program_id":
                    value = categorical_map.get(str(value))
                elif field == "currently_in_ct_program_id":
                    value = categorical_map.get(str(value))
            insert_obs(dest_cursor, person_id[0], encounter_id, config["concept_id"], value, config["type"], field)
    dest_conn.commit()
    src_cursor.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()
    print("Data successfully migrated to obs.")

if __name__ == "__main__":
    main()
