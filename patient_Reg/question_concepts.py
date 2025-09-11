import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import uuid

# ---------- CONFIG ----------
DB_CONFIG = {
    "host": "localhost",
    "user": "henryg",
    "password": "P@ssw0rd@1234",
    "database": "openmrs",
}
CREATOR_ID = 1
LOCALE = "en"

# ---------- INPUT DATA ----------
rows = [
    {"id": 35, "question": "How old were you when you had sex for the first time?", "Datatype": "number", "Class": "Question"},
    {"id": 36, "question": "How many sexual partners have you had in the last 12 months?", "Datatype": "number", "Class": "Question"},
    {"id": 145, "question": "Other Specify", "Datatype": "text", "Class": "Question"},
    {"id": 148, "question": "If Positive and Linked to ART, Indicate CCC #", "Datatype": "text", "Class": "Question"},
    {"id": 337, "question": "Comment on AGYW Eligible for a HIV test", "Datatype": "text", "Class": "Question"},
    {"id": 34, "question": "Have you ever had sex?", "Datatype": "Coded", "Class": "Question"},
    {"id": 37, "question": "Do you have a current sexual partner?", "Datatype": "Coded", "Class": "Question"},
    {"id": 38, "question": "Do you know this partner’s HIV status?", "Datatype": "Coded", "Class": "Question"},
    {"id": 39, "question": "How often did/do you use a condom with this partner?", "Datatype": "Coded", "Class": "Question"},
    {"id": 40, "question": "Had a condom burst?", "Datatype": "Coded", "Class": "Question"},
    {"id": 41, "question": "In a discordant relationship?", "Datatype": "Coded", "Class": "Question"},
    {"id": 42, "question": "In the last 12 months have you received money, gifts or favors in exchange for sex?", "Datatype": "Coded", "Class": "Question"},
    {"id": 43, "question": "Has the client engaged in sex under the influence of alcohol/drugs in the last 3Months?", "Datatype": "Coded", "Class": "Question"},
    {"id": 44, "question": "Has the client had an STI in the last 3 months?", "Datatype": "Coded", "Class": "Question"},
    {"id": 45, "question": "Is pregnant or breastfeeding? (If yes, refer for ANC)", "Datatype": "Coded", "Class": "Question"},
    {"id": 46, "question": "Has the client shared needles while injecting drugs in the last 3 month, if at all?", "Datatype": "Coded", "Class": "Question"},
    {"id": 47, "question": "Has the client used PEP in the last 3 months?", "Datatype": "Coded", "Class": "Question"},
    {"id": 48, "question": "Within the last 3 months, has the client experienced either physical or sexual violence?", "Datatype": "Coded", "Class": "Question"},
    {"id": 49, "question": "Have you ever been tested for HIV?", "Datatype": "Coded", "Class": "Question"},
    {"id": 142, "question": "When/how long ago was your last HIV test?", "Datatype": "Coded", "Class": "Question"},
    {"id": 143, "question": "If you don’t mind telling me, what were the results of your last HIV test?", "Datatype": "Coded", "Class": "Question"},
    {"id": 144, "question": "Why have you never been tested for HIV (Do not read the options) (Assess the risk, if they require the test, offer)", "Datatype": "Coded", "Class": "Question"},
    {"id": 149, "question": "If Negative, Don’t Know or Declined to disclose, Assess the risk, if they require the test, offer )", "Datatype": "Coded", "Class": "Question"},
    {"id": 151, "question": "HIV Prevention messages given to the AGYW (Tick)", "Datatype": "Coded", "Class": "Question"},
]

# ---------- HELPERS ----------
def ensure_log_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS concept_creation_log (
            id INT NOT NULL AUTO_INCREMENT,
            tools_question_id INT NULL,
            concept_id INT NULL,
            concept_uuid CHAR(38) NULL,
            name VARCHAR(1024) NOT NULL,
            action ENUM('created','linked_existing') NOT NULL,
            message VARCHAR(1024) NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY cq_tools_qid (tools_question_id),
            KEY cq_concept_id (concept_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

def log_action(cur, tools_q_id, concept_id, concept_uuid, name, action, message=None):
    cur.execute("""
        INSERT INTO concept_creation_log
            (tools_question_id, concept_id, concept_uuid, name, action, message)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (tools_q_id, concept_id, concept_uuid, name, action, message))

def get_id_by_name(cursor, table, name_col, target_col, value):
    cursor.execute(f"SELECT {target_col} FROM {table} WHERE LOWER({name_col}) = LOWER(%s) LIMIT 1", (value,))
    row = cursor.fetchone()
    return row[0] if row else None

def ensure_datatype_id(cursor, datatype_name):
    name_map = {
        "number": "Numeric",
        "numeric": "Numeric",
        "text": "Text",
        "coded": "Coded",
        "date": "Date",
        "datetime": "Datetime",
        "boolean": "Boolean",
    }
    lookup = name_map.get(str(datatype_name).strip().lower(), datatype_name)
    did = get_id_by_name(cursor, "concept_datatype", "name", "concept_datatype_id", lookup)
    if not did:
        raise ValueError(f"Datatype '{datatype_name}' not found in concept_datatype.")
    return did

def ensure_class_id(cursor, class_name):
    cid = get_id_by_name(cursor, "concept_class", "name", "concept_class_id", class_name)
    if not cid:
        raise ValueError(f"Class '{class_name}' not found in concept_class.")
    return cid

def find_existing_concept(cursor, fully_specified_name):
    cursor.execute("""
        SELECT c.concept_id, c.uuid
        FROM concept c
        JOIN concept_name cn ON cn.concept_id = c.concept_id
        WHERE cn.name = %s
          AND cn.concept_name_type = 'FULLY_SPECIFIED'
          AND cn.voided = 0
        LIMIT 1
    """, (fully_specified_name,))
    r = cursor.fetchone()
    return (r[0], r[1]) if r else (None, None)

def insert_concept(cursor, datatype_id, class_id):
    c_uuid = str(uuid.uuid4())
    cursor.execute("""
        INSERT INTO concept
            (datatype_id, class_id, is_set, creator, date_created, uuid, retired)
        VALUES
            (%s, %s, 0, %s, NOW(), %s, 0)
    """, (datatype_id, class_id, CREATOR_ID, c_uuid))
    return cursor.lastrowid, c_uuid

def insert_concept_name(cursor, concept_id, name):
    cn_uuid = str(uuid.uuid4())
    cursor.execute("""
        INSERT INTO concept_name
            (concept_id, name, locale, creator, date_created, concept_name_type,
             locale_preferred, uuid, voided)
        VALUES
            (%s, %s, %s, %s, NOW(), 'FULLY_SPECIFIED', 1, %s, 0)
    """, (concept_id, name, LOCALE, CREATOR_ID, cn_uuid))

def update_tools_questions(cursor, tools_question_id, concept_id):
    cursor.execute("""
        UPDATE tools_questions
        SET concept_id = %s
        WHERE id = %s
    """, (concept_id, tools_question_id))

# ---------- MAIN ----------
def main():
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()

        ensure_log_table(cur)

        created, linked = 0, 0
        for r in rows:
            tools_q_id = r["id"]
            name = r["question"].strip()
            datatype_id = ensure_datatype_id(cur, r["Datatype"])
            class_id = ensure_class_id(cur, r["Class"])

            existing_id, existing_uuid = find_existing_concept(cur, name)
            if existing_id:
                update_tools_questions(cur, tools_q_id, existing_id)
                log_action(cur, tools_q_id, existing_id, existing_uuid, name, "linked_existing",
                           "Concept name matched existing FULLY_SPECIFIED.")
                linked += 1
                print(f"[LINK] '{name}' -> concept_id={existing_id} (tools_questions.id={tools_q_id})")
                continue

            concept_id, concept_uuid = insert_concept(cur, datatype_id, class_id)
            insert_concept_name(cur, concept_id, name)
            update_tools_questions(cur, tools_q_id, concept_id)
            log_action(cur, tools_q_id, concept_id, concept_uuid, name, "created",
                       "New concept and FULLY_SPECIFIED name inserted.")
            created += 1
            print(f"[CREATE] '{name}' -> concept_id={concept_id} (tools_questions.id={tools_q_id})")

        conn.commit()
        print(f"\nDone. Created: {created}, Linked to existing: {linked}")

    except mysql.connector.Error as e:
        if conn:
            conn.rollback()
        if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("DB auth error: check user/password.")
        elif e.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(f"MySQL error: {e}")
    except Exception as ex:
        if conn:
            conn.rollback()
        print(f"Error: {ex}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
