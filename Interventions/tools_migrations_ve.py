#!/usr/bin/env python3
"""
Create OpenMRS visits & encounters per post_date for clients in tbl_tools_data.

- One Visit per (patient_id, post_date, visit_type_id, location_id)
- One Encounter per Visit (encounter_type, form_id)
- Idempotent (safe to re-run)
"""

import os
import uuid
import mysql.connector
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()

# -----------------------
# DB (from .env)
# -----------------------
DB = {
    'host': os.getenv('DEST_DB_HOST'),
    'user': os.getenv('DEST_DB_USER'),
    'password': os.getenv('DEST_DB_PASSWORD'),
    'database': os.getenv('DEST_DB_NAME')
}
# -----------------------
# CONFIG
# -----------------------
SOURCE_TABLE = "tbl_tools_data"   # has post_date, client_id, formcatalogue_id
FORMCATALOGUE_ID = 4              # filter if needed; set to None to include all
LOCATION_ID = 1
VISIT_TYPE_ID = 1                 # choose the visit type you want for these visits
ENCOUNTER_TYPE_ID = 470           # <-- your encounter type
FORM_ID = 552                     # <-- your form id
CREATOR_ID = 1
PROVIDER_ID = 1                   # set to None to skip encounter_provider
ENCOUNTER_ROLE_ID = 1             # required if PROVIDER_ID is set
UPSERT_MAPPING = True             # write tools_encounter_mapping (patient_id, encounter_id)

# -----------------------
# SQL
# -----------------------
SQL_DISTINCT_PATIENT_DATES = f"""
SELECT DISTINCT m.patient_id, d.post_date
FROM {SOURCE_TABLE} d
INNER JOIN dreams_client_patient_mapping m ON m.client_id = d.client_id
{"WHERE d.client_id IN (2314078,1152298)  AND d.formcatalogue_id = %s" if FORMCATALOGUE_ID is not None else ""}
"""

SQL_FIND_VISIT = """
SELECT visit_id
FROM visit
WHERE patient_id = %s
  AND visit_type_id = %s
  AND location_id = %s
  AND DATE(date_started) = DATE(%s)
  AND (date_stopped IS NULL OR DATE(date_stopped) = DATE(%s))
LIMIT 1
"""

SQL_INSERT_VISIT = """
INSERT INTO visit
(patient_id, visit_type_id, date_started, date_stopped, location_id, creator, date_created, uuid, voided)
VALUES
(%s, %s, %s, %s, %s, %s, NOW(), %s, 0)
"""

SQL_FIND_ENCOUNTER_BY_VISIT = """
SELECT encounter_id
FROM encounter
WHERE visit_id = %s
  AND patient_id = %s
  AND encounter_type = %s
  AND form_id = %s
LIMIT 1
"""

SQL_INSERT_ENCOUNTER = """
INSERT INTO encounter
(encounter_datetime, patient_id, encounter_type, form_id, visit_id, location_id, creator, date_created, uuid, voided)
VALUES
(%s, %s, %s, %s, %s, %s, %s, NOW(), %s, 0)
"""

SQL_INSERT_ENCOUNTER_PROVIDER = """
INSERT INTO encounter_provider
(encounter_id, provider_id, encounter_role_id, creator, date_created, uuid, voided)
VALUES
(%s, %s, %s, %s, NOW(), %s, 0)
"""

SQL_UPSERT_TOOLS_MAPPING = """
INSERT INTO tools_encounter_mapping (patient_id, encounter_id,form_id)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE encounter_id = VALUES(encounter_id)
"""

# -----------------------
# Helpers
# -----------------------
def as_date(your_date):
    """Return YYYY-MM-DD for date/datetime/str."""
    if isinstance(your_date, (datetime, date)):
        return your_date.strftime("%Y-%m-%d")
    s = str(your_date).strip()
    # MySQL DATE column in tbl_tools_data → already YYYY-MM-DD
    return s

def fetch_scalar(cur, sql, params):
    cur.execute(sql, params)
    r = cur.fetchone()
    return r[0] if r else None

def ensure_visit(cur, patient_id, post_date):
    """Get or create a Visit for the given patient/post_date."""
    ds = as_date(post_date)
    vid = fetch_scalar(cur, SQL_FIND_VISIT, (patient_id, VISIT_TYPE_ID, LOCATION_ID, ds, ds))
    if vid:
        return vid

    visit_uuid = str(uuid.uuid4())
    cur.execute(
        SQL_INSERT_VISIT,
        (patient_id, VISIT_TYPE_ID, ds, ds, LOCATION_ID, CREATOR_ID, visit_uuid)
    )
    cur.execute("SELECT LAST_INSERT_ID()")
    return cur.fetchone()[0]

def ensure_encounter(cur, patient_id, visit_id, post_date):
    """Get or create an Encounter under the Visit; attach provider if configured."""
    eid = fetch_scalar(cur, SQL_FIND_ENCOUNTER_BY_VISIT, (visit_id, patient_id, ENCOUNTER_TYPE_ID, FORM_ID))
    if eid:
        return eid

    enc_uuid = str(uuid.uuid4())
    cur.execute(
        SQL_INSERT_ENCOUNTER,
        (as_date(post_date), patient_id, ENCOUNTER_TYPE_ID, FORM_ID, visit_id, LOCATION_ID, CREATOR_ID, enc_uuid)
    )
    cur.execute("SELECT LAST_INSERT_ID()")
    eid = cur.fetchone()[0]

    if PROVIDER_ID is not None:
        cur.execute(
            SQL_INSERT_ENCOUNTER_PROVIDER,
            (eid, PROVIDER_ID, ENCOUNTER_ROLE_ID, CREATOR_ID, str(uuid.uuid4()))
        )
    return eid

def create_visits_and_encounters():
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    try:
        # 1) DISTINCT (patient_id, post_date) from tbl_tools_data ⨝ mapping
        if FORMCATALOGUE_ID is not None:
            cur.execute(SQL_DISTINCT_PATIENT_DATES, (FORMCATALOGUE_ID,))
        else:
            cur.execute(SQL_DISTINCT_PATIENT_DATES)
        rows = cur.fetchall()
        print(rows)

        created_visits = 0
        reused_visits  = 0
        created_encs   = 0
        reused_encs    = 0

        for patient_id, post_date in rows:
            # Visit
            vid = fetch_scalar(cur, SQL_FIND_VISIT, (patient_id, VISIT_TYPE_ID, LOCATION_ID, post_date, post_date))
            if vid is None:
                vid = ensure_visit(cur, patient_id, post_date)
                created_visits += 1
            else:
                reused_visits += 1

            # Encounter
            eid = fetch_scalar(cur, SQL_FIND_ENCOUNTER_BY_VISIT, (vid, patient_id, ENCOUNTER_TYPE_ID, FORM_ID))
            if eid is None:
                eid = ensure_encounter(cur, patient_id, vid, post_date)
                created_encs += 1
            else:
                reused_encs += 1

            # Optional mapping table upsert
            if UPSERT_MAPPING:
                try:
                    cur.execute(SQL_UPSERT_TOOLS_MAPPING, (patient_id, eid,FORM_ID))
                except mysql.connector.Error:
                    # If your tools_encounter_mapping has different columns, comment out UPSERT_MAPPING or adjust SQL
                    pass

        conn.commit()
        print(f"Visits  -> created: {created_visits}, reused: {reused_visits}")
        print(f"Encounters -> created: {created_encs}, reused: {reused_encs}")

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"ERROR: {e.msg}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_visits_and_encounters()
