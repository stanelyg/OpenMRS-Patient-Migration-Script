import pandas as pd
import mysql.connector
import uuid
from datetime import datetime, date
import os

DB_CONFIG = {
    'host': 'localhost',
    'user': 'henryg',
    'password': 'P@ssw0rd@1234',
    'database': 'openmrs'
}

# ---- Config you can tune ----
LOCATION_ID       = 1
VISIT_TYPE_ID     = 1             # pick the visit type you want
ENCOUNTER_TYPE_ID = 480           # your encounter type
FORM_ID           = 550           # your form id
CREATOR_ID        = 1
PROVIDER_ID       = 1             # set None to skip encounter_provider
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
    """Return 'YYYY-MM-DD' from date/datetime/str; fallback to today if None/empty."""
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

def fetch_scalar(cursor, sql, params):
    cursor.execute(sql, params)
    r = cursor.fetchone()
    return r[0] if r else None

def ensure_visit(cursor, patient_id, visit_date):
    """Find (or create) one Visit per patient/date (by type + location)."""
    sql_find = """
      SELECT visit_id FROM visit
      WHERE patient_id=%s AND visit_type_id=%s AND location_id=%s
        AND DATE(date_started)=DATE(%s)
        AND (date_stopped IS NULL OR DATE(date_stopped)=DATE(%s))
      LIMIT 1
    """
    vid = fetch_scalar(cursor, sql_find, (patient_id, VISIT_TYPE_ID, LOCATION_ID, visit_date, visit_date))
    if vid:
        return vid
    sql_ins = """
      INSERT INTO visit
      (patient_id, visit_type_id, date_started, date_stopped, location_id, creator, date_created, uuid, voided)
      VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, 0)
    """
    cursor.execute(sql_ins, (patient_id, VISIT_TYPE_ID, visit_date, visit_date, LOCATION_ID, CREATOR_ID, str(uuid.uuid4())))
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
    cursor.execute(sql_ins, (enc_datetime, patient_id, ENCOUNTER_TYPE_ID, FORM_ID, visit_id, LOCATION_ID, CREATOR_ID, str(uuid.uuid4())))
    eid = cursor.lastrowid
    if PROVIDER_ID is not None:
        cursor.execute("""
          INSERT INTO encounter_provider
          (encounter_id, provider_id, encounter_role_id, creator, date_created, uuid, voided)
          VALUES (%s, %s, %s, %s, NOW(), %s, 0)
        """, (eid, PROVIDER_ID, ENCOUNTER_ROLE_ID, CREATOR_ID, str(uuid.uuid4())))
    # Optional: log encounter creation
    try:
        cursor.execute("""
          INSERT INTO vuci_interventions_encounter_log
          (encounter_id, patient_id, visit_id, encounter_type, form_id, created_at)
          VALUES (%s, %s, %s, %s, %s, NOW())
        """, (eid, patient_id, visit_id, ENCOUNTER_TYPE_ID, FORM_ID))
    except mysql.connector.Error:
        pass
    return eid

def get_patient_id(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id=%s", (client_id,))
    r = cursor.fetchone()
    return r['patient_id'] if r else None

# ---------- NEW: existence checks to prevent duplicates ----------

SQL_EXISTS_GROUP = """
SELECT 1
FROM obs
WHERE person_id=%s AND encounter_id=%s AND concept_id=%s
  AND obs_group_id IS NULL AND voided=0
  AND DATE(obs_datetime)=DATE(%s)
LIMIT 1
"""

SQL_FIND_GROUP = """
SELECT obs_id
FROM obs
WHERE person_id=%s AND encounter_id=%s AND concept_id=%s
  AND obs_group_id IS NULL AND voided=0
  AND DATE(obs_datetime)=DATE(%s)
LIMIT 1
"""

SQL_EXISTS_CODED = """
SELECT 1
FROM obs
WHERE person_id=%s AND concept_id=%s AND encounter_id=%s
  AND obs_group_id=%s AND voided=0
  AND DATE(obs_datetime)=DATE(%s)
  AND value_coded=%s
LIMIT 1
"""

SQL_EXISTS_TEXT = """
SELECT 1
FROM obs
WHERE person_id=%s AND concept_id=%s AND encounter_id=%s
  AND obs_group_id=%s AND voided=0
  AND DATE(obs_datetime)=DATE(%s)
  AND value_text=%s
LIMIT 1
"""

SQL_EXISTS_DATE = """
SELECT 1
FROM obs
WHERE person_id=%s AND concept_id=%s AND encounter_id=%s
  AND obs_group_id=%s AND voided=0
  AND DATE(obs_datetime)=DATE(%s)
  AND DATE(value_datetime)=DATE(%s)
LIMIT 1
"""

SQL_EXISTS_NUM = """
SELECT 1
FROM obs
WHERE person_id=%s AND concept_id=%s AND encounter_id=%s
  AND obs_group_id=%s AND voided=0
  AND DATE(obs_datetime)=DATE(%s)
  AND value_numeric=%s
LIMIT 1
"""

def ensure_obs_group(cursor, person_id, encounter_id, obs_datetime):
    """Return an obs_group_id for the parent row; create if missing."""
    # Try to reuse existing group for same day/encounter/concept
    grp = fetch_scalar(cursor, SQL_FIND_GROUP, (person_id, encounter_id, group_concept_id, obs_datetime))
    if grp:
        return grp
    # Create parent only if a similar one doesn't exist
    exists = fetch_scalar(cursor, SQL_EXISTS_GROUP, (person_id, encounter_id, group_concept_id, obs_datetime))
    if exists:
        # re-fetch to return the id (race-safe enough for ETL)
        grp = fetch_scalar(cursor, SQL_FIND_GROUP, (person_id, encounter_id, group_concept_id, obs_datetime))
        if grp:
            return grp
    # Insert new parent obs
    obs_uuid = str(uuid.uuid4())
    cursor.execute("""
        INSERT INTO obs (uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
                         creator, date_created, voided)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), 0)
    """, (obs_uuid, person_id, group_concept_id, encounter_id, obs_datetime, LOCATION_ID, CREATOR_ID))
    return cursor.lastrowid

def insert_obs_dedup(cursor, person_id, encounter_id, concept_id, value, value_type, obs_group_id, obs_datetime):
    """Insert child obs only if an identical one doesn't already exist."""
    if value in (None, ""):
        return False

    if value_type == "coded":
        exists = fetch_scalar(cursor, SQL_EXISTS_CODED,
                              (person_id, concept_id, encounter_id, obs_group_id, obs_datetime, value))
        if exists:
            return False
        value_field = "value_coded"
        value_for_insert = value

    elif value_type == "text":
        exists = fetch_scalar(cursor, SQL_EXISTS_TEXT,
                              (person_id, concept_id, encounter_id, obs_group_id, obs_datetime, str(value)))
        if exists:
            return False
        value_field = "value_text"
        value_for_insert = str(value)

    elif value_type == "date":
        d = to_datestr(value)
        exists = fetch_scalar(cursor, SQL_EXISTS_DATE,
                              (person_id, concept_id, encounter_id, obs_group_id, obs_datetime, d))
        if exists:
            return False
        value_field = "value_datetime"
        value_for_insert = d

    elif value_type == "numeric":
        try:
            num = float(value)
        except Exception:
            return False
        exists = fetch_scalar(cursor, SQL_EXISTS_NUM,
                              (person_id, concept_id, encounter_id, obs_group_id, obs_datetime, num))
        if exists:
            return False
        value_field = "value_numeric"
        value_for_insert = num

    else:
        return False

    obs_uuid = str(uuid.uuid4())
    cursor.execute(f"""
        INSERT INTO obs (uuid, person_id, concept_id, encounter_id, obs_datetime, location_id,
                         {value_field}, creator, date_created, voided, obs_group_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), 0, %s)
    """, (obs_uuid, person_id, concept_id, encounter_id, obs_datetime, LOCATION_ID, value_for_insert, CREATOR_ID, obs_group_id))
    return True

def get_patient_id(cursor, client_id):
    cursor.execute("SELECT patient_id FROM dreams_client_patient_mapping WHERE client_id=%s", (client_id,))
    r = cursor.fetchone()
    return r['patient_id'] if r else None

# ------------------ main ------------------

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    # map for coded intervention types
    interventiontype_map = load_value_map(cursor, "dreamsapp_interventiontype")

    # Pull rows to migrate (adjust WHERE as you like)
    cursor.execute("""
        SELECT m.*
        FROM tbl_behavioural_interven m
        INNER JOIN dreams_client_patient_mapping cp ON cp.client_id = m.client_id
    """)
    rows = cursor.fetchall()

    inserted_rows = 0
    skipped_no_patient = 0

    # In-run dedup (protects against exact duplicates within the same run)
    seen_groups = set()   # (person_id, encounter_id, obs_date)  -> for parent creation
    seen_children = set() # (person_id, encounter_id, obs_date, concept_id, value_type, value_norm)

    for row in rows:
        client_id = row["client_id"]
        patient_id = get_patient_id(cursor, int(client_id))
        if not patient_id:
            skipped_no_patient += 1
            continue

        # driving date for visit/encounter/group/children
        enc_date = to_datestr(row.get("intervention_date"))

        # ensure Visit and Encounter for that date
        visit_id = ensure_visit(cursor, patient_id, enc_date)
        encounter_id = ensure_encounter(cursor, patient_id, visit_id, enc_date)

        # ensure parent/group obs (idempotent)
        gkey = (patient_id, encounter_id, enc_date)
        if gkey in seen_groups:
            # we still need the existing group's obs_id
            obs_group_id = fetch_scalar(cursor, SQL_FIND_GROUP, (patient_id, encounter_id, group_concept_id, enc_date))
            if not obs_group_id:
                obs_group_id = ensure_obs_group(cursor, patient_id, encounter_id, enc_date)
        else:
            obs_group_id = ensure_obs_group(cursor, patient_id, encounter_id, enc_date)
            seen_groups.add(gkey)

        # child obs (dedup: DB + in-run)
        for field, cfg in concept_map.items():
            value = row.get(field)
            if cfg["type"] == "coded" and field == "intervention_type_id":
                value = interventiontype_map.get(str(value))

            # build in-run key
            if cfg["type"] == "date":
                vnorm = to_datestr(value)
            elif cfg["type"] == "numeric":
                try:
                    vnorm = float(value) if value not in (None, "") else None
                except Exception:
                    vnorm = None
            else:
                vnorm = value if value is None else str(value).strip()

            ckey = (patient_id, encounter_id, enc_date, cfg["concept_id"], cfg["type"], vnorm)
            if vnorm not in (None, "") and ckey in seen_children:
                continue

            inserted = insert_obs_dedup(
                cursor,
                person_id=patient_id,
                encounter_id=encounter_id,
                concept_id=cfg["concept_id"],
                value=value if cfg["type"] != "date" else vnorm,
                value_type=cfg["type"],
                obs_group_id=obs_group_id,
                obs_datetime=enc_date,
            )
            if inserted:
                inserted_rows += 1
                seen_children.add(ckey)

    conn.commit()
    cursor.close()
    print(f"Done. Obs inserted (new): {inserted_rows}, skipped(no patient): {skipped_no_patient}")

if __name__ == "__main__":
    main()
