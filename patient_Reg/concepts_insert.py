import csv
import mysql.connector
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os
# DB connection config
DB_CONFIG = {
    'host': os.getenv('DEST_DB_HOST'),
    'user': os.getenv('DEST_DB_USER'),
    'password': os.getenv('DEST_DB_PASSWORD'),
    'database': os.getenv('DEST_DB_NAME')
}

# Fixed values
DATATYPE_ID = 4     # e.g., 1 for Numeric
CLASS_ID = 11        # e.g., 3 for Test
DESCRIPTION = "DREAMS Implementing Partner"
CREATOR_ID = 1
LOCALE = "en"

def concept_exists(cursor, name):
    cursor.execute("SELECT concept_id FROM concept_name WHERE name = %s", (name,))
    return cursor.fetchone() is not None

def insert_concept(cursor,source_id,name):
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

    # Log mapping
    cursor.execute("""
        INSERT INTO concept_migration_map (source_id, concept_id)
        VALUES (%s, %s)
    """, (source_id, concept_id))

    print(f"Inserted concept '{name}' (source ID: {source_id}) -> concept_id: {concept_id}")

    print(f"Inserted: {name}")

def bulk_insert(csv_file):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_id = int(row["id"].strip())
            name = row["name"].strip()
            if name:
                insert_concept(cursor, source_id, name)

    conn.commit()
    conn.close()
    print("All concepts processed.")

if __name__ == "__main__":
    bulk_insert("IP_concepts.csv")
