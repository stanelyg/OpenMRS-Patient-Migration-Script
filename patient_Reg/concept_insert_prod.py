import mysql.connector
import uuid
from datetime import datetime

# DB connection config
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

# Fixed values
DATATYPE_ID = 4        # Example: Coded
CLASS_ID = 11          # Example: Drug
DESCRIPTION = "DREAMS Interventions"
CREATOR_ID = 1
LOCALE = "en"

def concept_exists(cursor, name):
    cursor.execute("SELECT concept_id FROM concept_name WHERE name = %s", (name,))
    return cursor.fetchone() is not None

def insert_concept(cursor, source_id, name):
    if not name:
        return
    if concept_exists(cursor, name):
        print(f"Skipping existing concept: {name}")
        return

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    concept_uuid = str(uuid.uuid4())
    name_uuid = str(uuid.uuid4())
    desc_uuid = str(uuid.uuid4())

    # Insert into concept
    cursor.execute("""
        INSERT INTO concept (uuid, datatype_id, class_id, is_set, creator, date_created, version)
        VALUES (%s, %s, %s, 0, %s, %s, '1.0')
    """, (concept_uuid, DATATYPE_ID, CLASS_ID, CREATOR_ID, now))
    concept_id = cursor.lastrowid

    # Insert into concept_name
    cursor.execute("""
        INSERT INTO concept_name (uuid, concept_id, name, locale, locale_preferred, concept_name_type, creator, date_created)
        VALUES (%s, %s, %s, %s, 1, 'FULLY_SPECIFIED', %s, %s)
    """, (name_uuid, concept_id, name, LOCALE, CREATOR_ID, now))

    # Insert into concept_description
    cursor.execute("""
        INSERT INTO concept_description (uuid, concept_id, description, locale, creator, date_created)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (desc_uuid, concept_id, DESCRIPTION, LOCALE, CREATOR_ID, now))

    # Log in mapping table
    # cursor.execute("""
    #     INSERT INTO bio_medical_concept_migration_map (source_id, concept_id)
    #     VALUES (%s, %s)
    # """, (source_id, concept_id))

     # 🆕 Update the dreamsapp_interventiontype table with the new concept_id
    cursor.execute("""
        UPDATE dreamsapp_interventiontype
        SET concept_id = %s
        WHERE id = %s
    """, (concept_id, source_id))

    print(f"Inserted concept '{name}' (source ID: {source_id}) → concept_id: {concept_id}")

def bulk_insert_from_table():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    # Read from source table
    cursor.execute("SELECT id, name FROM concepts_to_create")
    rows = cursor.fetchall()

    for row in rows:
        source_id = row["id"]
        name = row["name"].strip()
        insert_concept(cursor, source_id, name)

    # Truncate source table
    cursor.execute("TRUNCATE TABLE concepts_to_create")

    conn.commit()
    conn.close()
    print("All concepts inserted and source table truncated.")

if __name__ == "__main__":
    bulk_insert_from_table()
