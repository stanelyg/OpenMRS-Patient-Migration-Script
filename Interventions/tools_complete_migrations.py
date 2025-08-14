#!/usr/bin/env python3
"""
OpenMRS ETL: create/reuse Visit+Encounter per post_date, then insert Obs.

- Visit: one per (patient_id, post_date, VISIT_TYPE_ID, LOCATION_ID)
- Encounter: one per Visit (ENCOUNTER_TYPE_ID, FORM_ID)
- Obs insert rules:
    * Duplicate (question_id, option_item_id) -> once
    * Multi-select: same question_id with different option_item_id -> all
    * text_response that's an integer -> value_numeric, else value_text
- Idempotent (checks existing obs)
"""

import os
import re
import uuid
from datetime import datetime, date
import mysql.connector
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
SOURCE_TABLE        = "tbl_tools_data"      # has post_date, client_id, formcatalogue_id, text/option/question
FORMCATALOGUE_ID    = 4                     # filter dataset; set to None to use all
LOCATION_ID         = 1
VISIT_TYPE_ID       = 1
ENCOUNTER_TYPE_ID   = 519
FORM_ID             = 553
CREATOR_ID          = 1
PROVIDER_ID         = 1        # set to None to skip encounter_provider
ENCOUNTER_ROLE_ID   = 1        # required if PROVIDER_ID is set
ENABLE_LOG          = True    # set True and adjust LOG_TABLE to log inserts
LOG_TABLE           = "obs_hts_eligibilty_log"   # columns: (obs_id, person_id, encounter_id, concept_id, field_name, value, logged_at)

# -----------------------
# SQL
# -----------------------
SQL_ROWS = f"""
SELECT id, post_date, client_id, text_response, option_item_id, question_id
FROM {SOURCE_TABLE}
{"WHERE formcatalogue_id = %s" if FORMCATALOGUE_ID is not None else ""}
ORDER BY id
"""

SQL_PATIENT = """
SELECT patient_id
FROM dreams_client_patient_mapping
WHERE client_id = %s
"""
#tbl_Questions_migration -> For production revert to this 
SQL_Q_CONCEPT = """
SELECT CAST(concept_id AS UNSIGNED) FROM tbl_Questions_migration WHERE id = %s 
"""
#tbl_response_options -> For production revert to this 
SQL_A_CONCEPT = """
SELECT concept_id FROM tbl_response_options  WHERE id = %s
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

SQL_EXISTS_CODED = """
SELECT 1 FROM obs
WHERE person_id=%s AND concept_id=%s AND obs_datetime=%s
  AND value_coded=%s AND encounter_id=%s AND voided=0
LIMIT 1
"""

SQL_EXISTS_TEXT = """
SELECT 1 FROM obs
WHERE person_id=%s AND concept_id=%s AND obs_datetime=%s
  AND value_text=%s AND encounter_id=%s AND voided=0
LIMIT 1
"""

SQL_EXISTS_NUMERIC = """
SELECT 1 FROM obs
WHERE person_id=%s AND concept_id=%s AND obs_datetime=%s
  AND value_numeric=%s AND encounter_id=%s AND voided=0
LIMIT 1
"""

SQL_INSERT_OBS_CODED = """
INSERT INTO obs
(person_id, concept_id, encounter_id, obs_datetime, location_id, value_coded, date_created, creator, uuid, voided)
VALUES
(%s, %s, %s, %s, %s, %s, NOW(), %s, %s, 0)
"""

SQL_INSERT_OBS_TEXT = """
INSERT INTO obs
(person_id, concept_id, encounter_id, obs_datetime, location_id, value_text, date_created, creator, uuid, voided)
VALUES
(%s, %s, %s, %s, %s, %s, NOW(), %s, %s, 0)
"""

SQL_INSERT_OBS_NUMERIC = """
INSERT INTO obs
(person_id, concept_id, encounter_id, obs_datetime, location_id, value_numeric, date_created, creator, uuid, voided)
VALUES
(%s, %s, %s, %s, %s, %s, NOW(), %s, %s, 0)
"""

# -----------------------
# Helpers
# -----------------------
_INT_RE = re.compile(r"^[+-]?\d+$")

def parse_int_or_none(s):
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    if _INT_RE.match(t):
        try:
            return int(t)
        except ValueError:
            return None
    return None

def as_date_str(v):
    if isinstance(v, (datetime, date)):
        return v.strftime("%Y-%m-%d")
    return str(v)

def fetch1(cur, sql, params):
    cur.execute(sql, params)
    r = cur.fetchone()
    return r[0] if r else None

def ensure_visit(cur, patient_id, dstr):
    vid = fetch1(cur, SQL_FIND_VISIT, (patient_id, VISIT_TYPE_ID, LOCATION_ID, dstr, dstr))
    if vid:
        return vid
    cur.execute(SQL_INSERT_VISIT, (patient_id, VISIT_TYPE_ID, dstr, dstr, LOCATION_ID, CREATOR_ID, str(uuid.uuid4())))
    cur.execute("SELECT LAST_INSERT_ID()")
    return cur.fetchone()[0]

def ensure_encounter(cur, patient_id, visit_id, dstr):
    eid = fetch1(cur, SQL_FIND_ENCOUNTER_BY_VISIT, (visit_id, patient_id, ENCOUNTER_TYPE_ID, FORM_ID))
    if eid:
        return eid
    cur.execute(
        SQL_INSERT_ENCOUNTER,
        (dstr, patient_id, ENCOUNTER_TYPE_ID, FORM_ID, visit_id, LOCATION_ID, CREATOR_ID, str(uuid.uuid4()))
    )
    cur.execute("SELECT LAST_INSERT_ID()")
    eid = cur.fetchone()[0]
    if ENABLE_LOG:
        try:
            cur.execute(
                f"INSERT INTO tools_encounter_mapping (encounter_id, patient_id,form_id) "
                "VALUES (%s, %s, %s, %s, %s, NOW())",
                (eid, patient_id,FORM_ID)
            )
        except mysql.connector.Error:
            pass

    if PROVIDER_ID is not None:
        cur.execute(
            SQL_INSERT_ENCOUNTER_PROVIDER,
            (eid, PROVIDER_ID, ENCOUNTER_ROLE_ID, CREATOR_ID, str(uuid.uuid4()))
        )
    return eid

def insert_obs_coded(cur, person_id, q_concept, enc_id, when_dt, a_concept):
    cur.execute(SQL_EXISTS_CODED, (person_id, q_concept, when_dt, a_concept, enc_id))
    if cur.fetchone():
        return False
    cur.execute(
        SQL_INSERT_OBS_CODED,
        (person_id, q_concept, enc_id, when_dt, LOCATION_ID, a_concept, CREATOR_ID, str(uuid.uuid4()))
    )
    return True

def insert_obs_text(cur, person_id, q_concept, enc_id, when_dt, text_value):
    cur.execute(SQL_EXISTS_TEXT, (person_id, q_concept, when_dt, text_value, enc_id))
    if cur.fetchone():
        return False
    cur.execute(
        SQL_INSERT_OBS_TEXT,
        (person_id, q_concept, enc_id, when_dt, LOCATION_ID, text_value, CREATOR_ID, str(uuid.uuid4()))
    )
    return True

def insert_obs_numeric(cur, person_id, q_concept, enc_id, when_dt, int_value):
    cur.execute(SQL_EXISTS_NUMERIC, (person_id, q_concept, when_dt, int_value, enc_id))
    if cur.fetchone():
        return False
    cur.execute(
        SQL_INSERT_OBS_NUMERIC,
        (person_id, q_concept, enc_id, when_dt, LOCATION_ID, int_value, CREATOR_ID, str(uuid.uuid4()))
    )
    return True

# -----------------------
# Main
# -----------------------
def run():
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor()

    # Load source rows
    if FORMCATALOGUE_ID is not None:
        cur.execute(SQL_ROWS, (FORMCATALOGUE_ID,))
    else:
        cur.execute(SQL_ROWS)
    rows = cur.fetchall()

    # In-run dedup keys (avoid redundant work within same execution)
    seen_coded   = set()  # (client_id, question_id, option_item_id, post_date)
    seen_numeric = set()  # (client_id, question_id, int_value, post_date)
    seen_text    = set()  # (client_id, question_id, norm_text, post_date)

    inserted = reused = skipped_map = 0

    try:
        for _row_id, post_date, client_id, text_response, option_item_id, question_id in rows:
            dstr = as_date_str(post_date)

            # map client -> patient
            patient_id = fetch1(cur, SQL_PATIENT, (client_id,))
            if not patient_id:
                skipped_map += 1
                continue

            # ensure visit & encounter for this date
            visit_id = ensure_visit(cur, patient_id, dstr)
            enc_id   = ensure_encounter(cur, patient_id, visit_id, dstr)

            # question -> concept
            q_concept = fetch1(cur, SQL_Q_CONCEPT, (question_id,))
            if not q_concept:
                skipped_map += 1
                continue

            did_insert = False

            # coded?
            if option_item_id is not None:
                a_concept = fetch1(cur, SQL_A_CONCEPT, (option_item_id,))
                if not a_concept:
                    skipped_map += 1
                    continue
                k = (client_id, question_id, int(option_item_id), dstr)
                if k in seen_coded:
                    reused += 1
                else:
                    seen_coded.add(k)
                    if insert_obs_coded(cur, patient_id, q_concept, enc_id, dstr, a_concept):
                        did_insert = True

            # text?
            elif text_response and str(text_response).strip():
                raw = str(text_response).strip()
                ival = parse_int_or_none(raw)
                if ival is not None:
                    k = (client_id, question_id, ival, dstr)
                    if k in seen_numeric:
                        reused += 1
                    else:
                        seen_numeric.add(k)
                        if insert_obs_numeric(cur, patient_id, q_concept, enc_id, dstr, ival):
                            did_insert = True
                else:
                    k = (client_id, question_id, raw, dstr)
                    if k in seen_text:
                        reused += 1
                    else:
                        seen_text.add(k)
                        if insert_obs_text(cur, patient_id, q_concept, enc_id, dstr, raw):
                            did_insert = True
            else:
                # empty row; skip silently
                pass

            if did_insert and ENABLE_LOG:
                try:
                    cur.execute(
                        f"INSERT INTO {LOG_TABLE} (obs_id, person_id, encounter_id, concept_id, field_name, value, logged_at) "
                        "VALUES (LAST_INSERT_ID(), %s, %s, %s, %s, %s, NOW())",
                        (patient_id, enc_id, q_concept,
                         "value_coded" if option_item_id is not None else ("value_numeric" if parse_int_or_none(text_response) is not None else "value_text"),
                         str(option_item_id if option_item_id is not None else text_response).strip() if text_response is not None else str(option_item_id))
                    )
                except mysql.connector.Error:
                    # ignore if log table not present/mismatched
                    pass

        conn.commit()
        print(f"Obs inserted: {inserted} (tracked internally), reused/dupe in-run: {reused}, skipped map: {skipped_map}")

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"ERROR: {e.msg}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run()
