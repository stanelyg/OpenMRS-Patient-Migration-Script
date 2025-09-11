import mysql.connector
import uuid
from datetime import datetime, date

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

# ---- Config ----
LOCATION_ID       = 1
VISIT_TYPE_ID     = 1
ENCOUNTER_TYPE_ID = 483        # adjust to your social protection encounter_type_id
FORM_ID           = 527        # adjust to your social protection form_id
CREATOR_ID        = 1
PROVIDER_ID       = 1
ENCOUNTER_ROLE_ID = 1

concept_map = {
    "intervention_type_id": {"concept_id": 1000880, "type": "coded"},
    "intervention_date":    {"concept_id": 1000884, "type": "date"},
    "comment":              {"concept_id": 1000653, "type": "text"},
    "other_specify":        {"concept_id": 1001799, "type": "text"}
}

# ---------------- helpers ----------------

def to_datestr(v):
    if v is None or (isinstance(v, str) and not v.strip()):
        return datetime.today().strftime('%Y-%m-%d')
    if isinstance(v, (datetime, date)):
        return v.strftime('%Y-%m-%d')
    return str(v).split(" ")[0]

def load_value_map(cursor, table_name):
    cursor.execute(f"SELECT id, concept_id FROM {table_name}")
    return {str(row['id']): row['concept_id'] for row in cursor.fetchall()}

def get_patient_id(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id=%s", (client_id,))
    r = cursor.fetchone()
    return r['patient_id'] if r else None

def ensure_visit(cursor, patient_id, visit_date):
    cursor.execute("""
      SELECT visit_id FROM visit
      WHERE patient_id=%s AND visit_type_id=%s AND location_id=%s
        AND DATE(date_started)=DATE(%s)
      LIMIT 1
    """, (patient_id, VISIT_TYPE_ID, LOCATION_ID, visit_date))
    row = cursor.fetchone()
    if row:
        return row['visit_id'] if isinstance(row, dict) else row[0]
    cursor.execute("""
      INSERT INTO visit (patient_id, visit_type_id, date_started, date_stopped, location_id,
                         creator, date_created, uuid, voided)
      VALUES (%s,%s,%s,%s,%s,%s,NOW(),%s,0)
    """, (patient_id, VISIT_TYPE_ID, visit_date, visit_date, LOCATION_ID,
          CREATOR_ID, str(uuid.uuid4())))
    return cursor.lastrowid

def ensure_encounter(cursor, patient_id, visit_id, enc_date):
    cursor.execute("""
      SELECT encounter_id FROM encounter
      WHERE patient_id=%s AND visit_id=%s AND encounter_type=%s AND form_id=%s
      LIMIT 1
    """, (patient_id, visit_id, ENCOUNTER_TYPE_ID, FORM_ID))
    row = cursor.fetchone()
    if row:
        return row['encounter_id'] if isinstance(row, dict) else row[0]
    cursor.execute("""
      INSERT INTO encounter (encounter_datetime, patient_id, encounter_type, form_id,
                             visit_id, location_id, creator, date_created, uuid, voided)
      VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),%s,0)
    """, (enc_date, patient_id, ENCOUNTER_TYPE_ID, FORM_ID,
          visit_id, LOCATION_ID, CREATOR_ID, str(uuid.uuid4())))
    eid = cursor.lastrowid
    if PROVIDER_ID:
        cursor.execute("""
          INSERT INTO encounter_provider (encounter_id, provider_id, encounter_role_id,
                                          creator, date_created, uuid, voided)
          VALUES (%s,%s,%s,%s,NOW(),%s,0)
        """, (eid, PROVIDER_ID, ENCOUNTER_ROLE_ID, CREATOR_ID, str(uuid.uuid4())))
    return eid

def obs_exists(cursor, person_id, concept_id, encounter_id, value_field, value, obs_date):
    cursor.execute(f"""
      SELECT 1 FROM obs
      WHERE person_id=%s AND concept_id=%s AND encounter_id=%s
        AND {value_field}=%s AND DATE(obs_datetime)=DATE(%s) AND voided=0
      LIMIT 1
    """, (person_id, concept_id, encounter_id, value, obs_date))
    return cursor.fetchone() is not None

def insert_obs(cursor, person_id, encounter_id, concept_id, value, value_type, field_name, obs_date):
    if value in (None, ""):
        return
    field_map = {
        "coded": "value_coded",
        "text": "value_text",
        "date": "value_datetime",
        "numeric": "value_numeric",
    }
    value_field = field_map.get(value_type)
    if not value_field:
        return
    # normalize values
    if value_type == "date":
        value = to_datestr(value)
    elif value_type == "numeric":
        try:
            value = float(value)
        except Exception:
            return
    # duplicate check
    if obs_exists(cursor, person_id, concept_id, encounter_id, value_field, value, obs_date):
        print(f"Skipping duplicate obs for patient={person_id}, concept={concept_id}, value={value}")
        return
    obs_uuid = str(uuid.uuid4())
    cursor.execute(f"""
        INSERT INTO obs (uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
                         {value_field}, creator, date_created, voided)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),0)
    """, (obs_uuid, person_id, concept_id, encounter_id, obs_date,
          LOCATION_ID, value, CREATOR_ID))
    obs_id = cursor.lastrowid
    # log
    cursor.execute("""
        INSERT INTO obs_social_protection_log (obs_id, person_id, encounter_id, concept_id, field_name, value)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (obs_id, person_id, encounter_id, concept_id, field_name, str(value)))

# ---------------- main ----------------

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    interventiontype_map = load_value_map(cursor, "dreamsapp_interventiontype")

    cursor.execute("""
        SELECT m.* FROM tbl_social_protection_interventions m
        INNER JOIN dreams_client_patient_mapping cp ON cp.client_id = m.client_id
    """)
    rows = cursor.fetchall()

    inserted_rows = 0
    skipped_no_patient = 0

    for row in rows:
        client_id = row["client_id"]
        patient_id = get_patient_id(cursor, int(client_id))
        if not patient_id:
            skipped_no_patient += 1
            continue

        enc_date = to_datestr(row.get("intervention_date"))
        visit_id = ensure_visit(cursor, patient_id, enc_date)
        encounter_id = ensure_encounter(cursor, patient_id, visit_id, enc_date)

        for field, cfg in concept_map.items():
            value = row.get(field)
            if cfg["type"] == "coded" and field == "intervention_type_id":
                value = interventiontype_map.get(str(value))
            insert_obs(cursor, patient_id, encounter_id, cfg["concept_id"],
                       value, cfg["type"], field, enc_date)
            inserted_rows += 1

    conn.commit()
    cursor.close()
    print(f"Social Protection data migrated. Obs inserted: {inserted_rows}, skipped(no patient): {skipped_no_patient}")

if __name__ == "__main__":
    main()
