import pandas as pd
import mysql.connector
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os

SOURCE_DB_CONFIG = {
    'host': os.getenv('SOURCE_DB_HOST'),
    'user': os.getenv('SOURCE_DB_USER'),
    'password': os.getenv('SOURCE_DB_PASSWORD'),
    'database': os.getenv('SOURCE_DB_NAME')
}

DEST_DB_CONFIG = {
    'host': os.getenv('DEST_DB_HOST'),
    'user': os.getenv('DEST_DB_USER'),
    'password': os.getenv('DEST_DB_PASSWORD'),
    'database': os.getenv('DEST_DB_NAME')
}

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
    "relationship_with_guardian": {"concept_id": 1000666, "type": "text"},
    "guardian_phone_number": {"concept_id": 1000667, "type": "text"},
    "guardian_national_id": {"concept_id": 1000668, "type": "text"},
    "external_organisation_id": {"concept_id": 1000669, "type": "coded"},
    "cpmis_id": {"concept_id": 1000670, "type": "text"},
    "nemis_no": {"concept_id": 1000671, "type": "text"},
    "nupi_no": {"concept_id": 1000672, "type": "text"}
}


# Load ID-to-concept mappings from lookup tables
def load_value_map(cursor, table_name):
    cursor.execute(f"SELECT id, concept_id FROM {table_name}")
    return {str(row[0]): row[1] for row in cursor.fetchall()}


def get_person_and_encounter(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id = %s", (client_id,))
    row = cursor.fetchone()
    if not row:
        return None, None, None
    patient_id = row
    cursor.execute("SELECT encounter_id FROM patient_encounter_mapping WHERE patient_id = %s", patient_id)
    encounter_row = cursor.fetchone()
    encounter_id = encounter_row[0] if encounter_row else None
    return patient_id, patient_id, encounter_id

def cast_to_number(value):
    try:
        num = float(value)
        if num.is_integer():
            return int(num)
        return num
    except (ValueError, TypeError):
        return value

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
    src_conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    dest_conn = mysql.connector.connect(**DEST_DB_CONFIG)
    src_cursor = src_conn.cursor(dictionary=True)
    dest_cursor = dest_conn.cursor()

    # Load mapping tables
    ip_map = load_value_map(dest_cursor, "dreamsapp_implementingpartner")
    doc_ver_map = load_value_map(dest_cursor, "DreamsApp_verificationdocument_mapping")
    marital_status_map = load_value_map(dest_cursor, "DreamsApp_maritalstatus_mapping")
    county_map = load_value_map(dest_cursor, "dreamsapp_county")
    subcounty_map = load_value_map(dest_cursor, "dreamsapp_subcounty")
    ward_map = load_value_map(dest_cursor, "dreamsapp_ward")
    ext_org_map = load_value_map(dest_cursor, "dreamsapp_externalorganisation")

    # Read source data
    src_cursor.execute("SELECT * FROM tbl_m_demographics")
    for row in src_cursor.fetchall():
        client_id = row["client_id"]
        person_id, patient_id, encounter_id = get_person_and_encounter(dest_cursor, client_id)
        if not person_id or not encounter_id:
            print(f"Skipping client_id {client_id} - missing person or encounter")
            continue

        for field, config in concept_map.items():
            value = cast_to_number(row.get(field))
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

            insert_obs(dest_cursor, person_id, encounter_id, config["concept_id"], value, config["type"], field)

    dest_conn.commit()
    src_cursor.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()
    print("✅ Data successfully migrated to `obs`.")

if __name__ == "__main__":
    main()