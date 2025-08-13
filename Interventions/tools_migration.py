#!/usr/bin/env python3
"""
ETL: staging table -> OpenMRS obs (NO encounter creation).
- Dedup per rules:
    * (question_id, option_item_id) duplicates -> insert once
    * same question_id with different option_item_id -> insert all (multi-select)
    * duplicate text (normalized) -> insert once
- text_response that is an integer -> value_numeric, else value_text
"""

import os
import re
import uuid
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# -----------------------
# DB connection from .env
# -----------------------
DB = {
    'host': os.getenv('DEST_DB_HOST'),
    'user': os.getenv('DEST_DB_USER'),
    'password': os.getenv('DEST_DB_PASSWORD'),
    'database': os.getenv('DEST_DB_NAME')
}

# -----------------------
# Script constants
# -----------------------
SOURCE_TABLE   = "tbl_tools_data"         # staging table in openmrs DB
MAPPING_TABLE  = "tools_encounter_mapping"     # patient_id -> encounter_id
CREATOR_ID     = 1
LOCATION_ID    = 1
ENABLE_LOG     = True
VERIFY_MAPPED_ENCOUNTER = True                 # ensure mapped encounter exists & belongs to patient

# -----------------------
# SQL
# -----------------------
SQL_ALL_SOURCE = f"""
SELECT id, post_date, client_id, text_response, option_item_id, question_id
FROM {SOURCE_TABLE} WHERE formcatalogue_id=4
ORDER BY id
"""

SQL_PATIENT = """
SELECT patient_id
FROM dreams_client_patient_mapping
WHERE client_id = %s
"""

SQL_Q_CONCEPT = """
SELECT CAST(concept_id AS UNSIGNED)
FROM tbl_Questions_migration
WHERE id = %s
"""

SQL_A_CONCEPT = """
SELECT concept_id
FROM tbl_response_options
WHERE id = %s
"""

SQL_MAP_ENCOUNTER = f"""
SELECT encounter_id
FROM {MAPPING_TABLE}
WHERE patient_id = %s
LIMIT 1
"""

SQL_VERIFY_ENCOUNTER = """
SELECT 1
FROM encounter
WHERE encounter_id = %s AND patient_id = %s AND voided = 0
LIMIT 1
"""

SQL_EXISTS_CODED = """
SELECT 1
FROM obs
WHERE person_id=%s AND concept_id=%s AND obs_datetime=%s
  AND value_coded=%s AND encounter_id=%s AND voided=0
LIMIT 1
"""

SQL_EXISTS_TEXT = """
SELECT 1
FROM obs
WHERE person_id=%s AND concept_id=%s AND obs_datetime=%s
  AND value_text=%s AND encounter_id=%s AND voided=0
LIMIT 1
"""

SQL_EXISTS_NUMERIC = """
SELECT 1
FROM obs
WHERE person_id=%s AND concept_id=%s AND obs_datetime=%s
  AND value_numeric=%s AND encounter_id=%s AND voided=0
LIMIT 1
"""

SQL_INSERT_OBS_CODED = """
INSERT INTO obs
(person_id, concept_id, encounter_id, obs_datetime, location_id,
 value_coded, date_created, creator, uuid, voided)
VALUES
(%s, %s, %s, %s, %s, %s, NOW(), %s, %s, 0)
"""

SQL_INSERT_OBS_TEXT = """
INSERT INTO obs
(person_id, concept_id, encounter_id, obs_datetime, location_id,
 value_text, date_created, creator, uuid, voided)
VALUES
(%s, %s, %s, %s, %s, %s, NOW(), %s, %s, 0)
"""

SQL_INSERT_OBS_NUMERIC = """
INSERT INTO obs
(person_id, concept_id, encounter_id, obs_datetime, location_id,
 value_numeric, date_created, creator, uuid, voided)
VALUES
(%s, %s, %s, %s, %s, %s, NOW(), %s, %s, 0)
"""

SQL_LAST_ID = "SELECT LAST_INSERT_ID()"

# -----------------------
# Helpers
# -----------------------
_INT_RE = re.compile(r"^[+-]?\d+$")

def parse_int_if_possible(s: str):
    """Return int if s is an integer literal after trim, else None."""
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

def fetch_scalar(cur, sql, params):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None

def table_exists(cur, name: str) -> bool:
    cur.execute("SHOW TABLES LIKE %s", (name,))
    return cur.fetchone() is not None

def get_mapped_encounter_id(cur, patient_id, cache):
    if patient_id in cache:
        return cache[patient_id]
    enc_id = fetch_scalar(cur, SQL_MAP_ENCOUNTER, (patient_id,))
    cache[patient_id] = enc_id  # may be None; cache the miss too
    return enc_id

def verify_encounter(cur, encounter_id, patient_id) -> bool:
    if not VERIFY_MAPPED_ENCOUNTER:
        return True
    ok = fetch_scalar(cur, SQL_VERIFY_ENCOUNTER, (encounter_id, patient_id))
    return bool(ok)

def insert_obs_coded(cur, person_id, q_concept, enc_id, when_dt, a_concept, log_table):
    # DB-level idempotency
    cur.execute(SQL_EXISTS_CODED, (person_id, q_concept, when_dt, a_concept, enc_id))
    if cur.fetchone():
        return None
    obs_uuid = str(uuid.uuid4())
    cur.execute(SQL_INSERT_OBS_CODED, (person_id, q_concept, enc_id, when_dt, LOCATION_ID, a_concept, CREATOR_ID, obs_uuid))
    cur.execute(SQL_LAST_ID)
    obs_id = cur.fetchone()[0]
    if log_table:
        cur.execute(
            f"INSERT INTO {log_table} (obs_id, person_id, encounter_id, concept_id, field_name, value, logged_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,NOW())",
            (obs_id, person_id, enc_id, q_concept, "value_coded", str(a_concept)),
        )
    return obs_id

def insert_obs_text(cur, person_id, q_concept, enc_id, when_dt, text_value, log_table):
    cur.execute(SQL_EXISTS_TEXT, (person_id, q_concept, when_dt, text_value, enc_id))
    if cur.fetchone():
        return None
    obs_uuid = str(uuid.uuid4())
    cur.execute(SQL_INSERT_OBS_TEXT, (person_id, q_concept, enc_id, when_dt, LOCATION_ID, text_value, CREATOR_ID, obs_uuid))
    cur.execute(SQL_LAST_ID)
    obs_id = cur.fetchone()[0]
    if log_table:
        cur.execute(
            f"INSERT INTO {log_table} (obs_id, person_id, encounter_id, concept_id, field_name, value, logged_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,NOW())",
            (obs_id, person_id, enc_id, q_concept, "value_text", text_value),
        )
    return obs_id

def insert_obs_numeric(cur, person_id, q_concept, enc_id, when_dt, int_value, log_table):
    cur.execute(SQL_EXISTS_NUMERIC, (person_id, q_concept, when_dt, int_value, enc_id))
    if cur.fetchone():
        return None
    obs_uuid = str(uuid.uuid4())
    cur.execute(SQL_INSERT_OBS_NUMERIC, (person_id, q_concept, enc_id, when_dt, LOCATION_ID, int_value, CREATOR_ID, obs_uuid))
    cur.execute(SQL_LAST_ID)
    obs_id = cur.fetchone()[0]
    if log_table:
        cur.execute(
            f"INSERT INTO {log_table} (obs_id, person_id, encounter_id, concept_id, field_name, value, logged_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,NOW())",
            (obs_id, person_id, enc_id, q_concept, "value_numeric", str(int_value)),
        )
    return obs_id

# -----------------------
# Main
# -----------------------
def run():
    conn = mysql.connector.connect(**DB)
    cur  = conn.cursor()

    log_table ='obs_hts_eligibilty_log'

    cur.execute(SQL_ALL_SOURCE)
    rows = cur.fetchall()

    mapping_cache = {}

    # In-run dedup (to avoid repeated DB checks for exact duplicates in the same batch)
    seen_coded   = set()  # (person_id, q_concept, enc_id, post_date, a_concept)
    seen_numeric = set()  # (person_id, q_concept, enc_id, post_date, int_value)
    seen_text    = set()  # (person_id, q_concept, enc_id, post_date, normalized_text)

    inserted = skipped_map = skipped_empty = skipped_no_enc = skipped_bad_enc = skipped_dupe = 0

    try:
        for row_id, post_date, client_id, text_response, option_item_id, question_id in rows:
            # client -> patient
            patient_id = fetch_scalar(cur, SQL_PATIENT, (client_id,))
            if not patient_id:
                skipped_map += 1
                continue

            # patient -> encounter (from mapping table)
            enc_id = get_mapped_encounter_id(cur, patient_id, mapping_cache)
            if not enc_id:
                skipped_no_enc += 1
                continue
            if not verify_encounter(cur, enc_id, patient_id):
                skipped_bad_enc += 1
                continue

            # question -> concept
            q_concept = fetch_scalar(cur, SQL_Q_CONCEPT, (question_id,))
            if not q_concept:
                skipped_map += 1
                continue

            # Coded vs Text
            if option_item_id is not None:
                # Multi-select rule: allow multiple distinct option_item_id for the same question
                a_concept = fetch_scalar(cur, SQL_A_CONCEPT, (option_item_id,))
                if not a_concept:
                    skipped_map += 1
                    continue

                k = (patient_id, q_concept, enc_id, post_date, a_concept)
                if k in seen_coded:
                    skipped_dupe += 1
                    continue
                seen_coded.add(k)

                if insert_obs_coded(cur, patient_id, q_concept, enc_id, post_date, a_concept, log_table):
                    inserted += 1

            elif text_response and str(text_response).strip():
                raw = str(text_response).strip()

                # If integer, save as value_numeric; else as value_text
                int_val = parse_int_if_possible(raw)
                if int_val is not None:
                    k = (patient_id, q_concept, enc_id, post_date, int_val)
                    if k in seen_numeric:
                        skipped_dupe += 1
                        continue
                    seen_numeric.add(k)

                    if insert_obs_numeric(cur, patient_id, q_concept, enc_id, post_date, int_val, log_table):
                        inserted += 1
                else:
                    k = (patient_id, q_concept, enc_id, post_date, raw)
                    if k in seen_text:
                        skipped_dupe += 1
                        continue
                    seen_text.add(k)

                    if insert_obs_text(cur, patient_id, q_concept, enc_id, post_date, raw, log_table):
                        inserted += 1
            else:
                skipped_empty += 1

        conn.commit()

    except mysql.connector.Error as e:
        conn.rollback()
        print(f"ERROR: {e.msg}")
    finally:
        cur.close()
        conn.close()

    print(
        f"Inserted obs: {inserted}, "
        f"skipped(missing maps): {skipped_map}, "
        f"skipped(empty): {skipped_empty}, "
        f"skipped(no encounter mapping): {skipped_no_enc}, "
        f"skipped(bad encounter mapping): {skipped_bad_enc}, "
        f"skipped(dupes this run): {skipped_dupe}"
    )

if __name__ == "__main__":
    run()
