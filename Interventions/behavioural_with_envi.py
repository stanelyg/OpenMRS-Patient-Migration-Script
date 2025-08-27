import mysql.connector
import uuid
from datetime import datetime, date
import os

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

# ---- Config ----
LOCATION_ID       = 1
VISIT_TYPE_ID     = 1
ENCOUNTER_TYPE_ID = 480   # behavioural encounter type
FORM_ID           = 550
CREATOR_ID        = 1
PROVIDER_ID       = 1
ENCOUNTER_ROLE_ID = 1

concept_map = {
    "intervention_type_id": {"concept_id": 1000880, "type": "coded"},
    "intervention_date":    {"concept_id": 1000884, "type": "date"},
    "comment":              {"concept_id": 1000653, "type": "text"},
    "other_specify":        {"concept_id": 1001774, "type": "text"},
}

group_concept_id = 1001775  # parent/group obs concept

# ------------------ helpers ------------------

def to_datestr(v):
    if v is None or (isinstance(v, str) and not v.strip()):
        return datetime.today().strftime('%Y-%m-%d')
    if isinstance(v, (datetime, date)):
        return v.strftime('%Y-%m-%d')
    s = str(v).strip()
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return s

def load_value_map(cursor, table_name):
    cursor.execute(f"SELECT id, concept_id FROM {table_name}")
    return {str(row['id']): row['concept_id'] for row in cursor.fetchall()}

def fetch_scalar(cur, sql, params):
    cur.execute(sql, params)
    r = cur.fetchone()
    if not r:
        return None
    if isinstance(r, dict):        # dictionary cursor
        return list(r.values())[0]
    return r[0]

def ensure_visit(cursor, patient_id, visit_date):
    """Find (or create) one Visit per patient/date (by type + location)."""
    sql_find = """
      SELECT visit_id FROM visit
      WHERE patient_id=%s AND visit_type_id=%s AND location_id=%s
        AND DATE(date_started)=DATE(%s)
      LIMIT 1
    """
    vid = fetch_scalar(cursor, sql_find, (patient_id, VISIT_TYPE_ID, LOCATION_ID, visit_date))
    if vid:
        return vid
    sql_ins = """
      INSERT INTO visit
      (patient_id, visit_type_id, date_started, date_stopped, location_id, creator, date_created, uuid, voided)
      VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, 0)
    """
    cursor.execute(sql_ins, (patient_id, VISIT_TYPE_ID, visit_date, visit_date,
                             LOCATION_ID, CREATOR_ID, str(uuid.uuid4())))
    return cursor.lastrowid

def ensure_encounter(cursor, patient_id, visit_id, enc_datetime):
    """Find (or create) one Encounter per visit (form + type)."""
    sql_find = """
      SELECT encounter_id FROM encounter
      WHERE visit_id=%s AND patient_id=%s AND encounter_type=%s AND form_id=%s
      LIMIT 1
    """
    eid = fetch_scalar(cursor, sql_find, (visit_id, patient_id, ENCOUNTER_TYPE_ID, FORM_ID))
    if eid:
        return eid
    sql_ins = """
      INSERT INTO encounter
      (encounter_datetime, patient_id, encounter_type, form_id, visit_id, location_id, creator, date_created, uuid, voided)
      VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, 0)
    """
    cursor.execute(sql_ins, (enc_datetime, patient_id, ENCOUNTER_TYPE_ID, FORM_ID,
                             visit_id, LOCATION_ID, CREATOR_ID, str(uuid.uuid4())))
    eid = cursor.lastrowid
    if PROVIDER_ID is not None:
        cursor.execute("""
          INSERT INTO encounter_provider
          (encounter_id, provider_id, encounter_role_id, creator, date_created, uuid, voided)
          VALUES (%s, %s, %s, %s, NOW(), %s, 0)
        """, (eid, PROVIDER_ID, ENCOUNTER_ROLE_ID, CREATOR_ID, str(uuid.uuid4())))
    # Optional encounter logging
    try:
        cursor.execute("""
          INSERT INTO vuci_interventions_encounter_log
          (encounter_id, patient_id, visit_id, encounter_type, form_id, created_at)
          VALUES (%s, %s, %s, %s, %s, NOW())
        """, (eid, patient_id, visit_id, ENCOUNTER_TYPE_ID, FORM_ID))
    except mysql.connector.Error:
        pass
    return eid

# ---------- existence checks ----------

SQL_EXISTS_OBS = """
SELECT 1
FROM obs
WHERE person_id=%s AND concept_id=%s AND encounter_id=%s
  AND obs_group_id=%s AND voided=0
  AND DATE(obs_datetime)=DATE(%s)
  AND {field}=%s
LIMIT 1
"""

def get_patient_id(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id=%s", (client_id,))
    r = cursor.fetchone()
    return r['patient_id'] if r else None

def ensure_obs_group(cursor, person_id, encounter_id, obs_datetime):
    """Always create a parent obs group for this encounter/date (idempotent)."""
    cursor.execute("""
        SELECT obs_id FROM obs
        WHERE person_id=%s AND encounter_id=%s AND concept_id=%s
          AND obs_group_id IS NULL AND voided=0
          AND DATE(obs_datetime)=DATE(%s)
        LIMIT 1
    """, (person_id, encounter_id, group_concept_id, obs_datetime))
    r = cursor.fetchone()
    if r:
        return r['obs_id'] if isinstance(r, dict) else r[0]
    obs_uuid = str(uuid.uuid4())
    cursor.execute("""
        INSERT INTO obs (uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
                         creator, date_created, voided)
        VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),0)
    """, (obs_uuid, person_id, group_concept_id, encounter_id, obs_datetime,
          LOCATION_ID, CREATOR_ID))
    return cursor.lastrowid

def insert_obs_dedup(cursor, person_id, encounter_id, concept_id, value, value_type,
                     obs_group_id, obs_datetime, field_name=None):
    """Insert obs only if not duplicate, and log in obs_behavioural_migration_log."""
    if value in (None, ""):
        return False

    field_map = {
        "coded": "value_coded",
        "text": "value_text",
        "date": "value_datetime",
        "numeric": "value_numeric",
    }
    value_field = field_map.get(value_type)
    if not value_field:
        return False

    # normalize date/numeric
    if value_type == "date":
        value = to_datestr(value)
    elif value_type == "numeric":
        try:
            value = float(value)
        except Exception:
            return False

    # check DB duplicate
    cursor.execute(SQL_EXISTS_OBS.format(field=value_field),
                   (person_id, concept_id, encounter_id, obs_group_id, obs_datetime, value))
    if cursor.fetchone():
        return False

    obs_uuid = str(uuid.uuid4())
    cursor.execute(f"""
        INSERT INTO obs (uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
                         {value_field}, creator, date_created, voided, obs_group_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),0,%s)
    """, (obs_uuid, person_id, concept_id, encounter_id, obs_datetime,
          LOCATION_ID, value, CREATOR_ID, obs_group_id))
    obs_id = cursor.lastrowid

    # log insert
    try:
        cursor.execute("""
            INSERT INTO obs_behavioural_migration_log
            (obs_id, person_id, encounter_id, concept_id, field_name, value, logged_at)
            VALUES (%s,%s,%s,%s,%s,%s,NOW())
        """, (obs_id, person_id, encounter_id, concept_id, field_name or value_field, str(value)))
    except mysql.connector.Error as e:
        print(f"⚠️ Logging failed for obs_id={obs_id}: {e}")

    return True

# ------------------ main ------------------

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    interventiontype_map = load_value_map(cursor, "dreamsapp_interventiontype")

    cursor.execute("""
        SELECT m.* FROM tbl_behavioural_interven m
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
        obs_group_id = ensure_obs_group(cursor, patient_id, encounter_id, enc_date)

        for field, cfg in concept_map.items():
            value = row.get(field)
            if cfg["type"] == "coded" and field == "intervention_type_id":
                value = interventiontype_map.get(str(value))
            inserted = insert_obs_dedup(
                cursor,
                person_id=patient_id,
                encounter_id=encounter_id,
                concept_id=cfg["concept_id"],
                value=value,
                value_type=cfg["type"],
                obs_group_id=obs_group_id,
                obs_datetime=enc_date,
                field_name=field
            )
            if inserted:
                inserted_rows += 1

    conn.commit()
    cursor.close()
    print(f"✅ Done. Obs inserted (new): {inserted_rows}, skipped(no patient): {skipped_no_patient}")

if __name__ == "__main__":
    main()
