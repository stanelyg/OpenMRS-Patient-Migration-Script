import mysql.connector
from multiprocessing import Pool, cpu_count
import math

# Database config
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

BATCH_SIZE = 250000  # adjust for performance

def get_total_encounters():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM patient_encounter_mapping")
    total = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return total

def get_encounter_ids_batch(offset, limit):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT encounter_id 
        FROM patient_encounter_mapping 
        ORDER BY encounter_id 
        LIMIT %s OFFSET %s
    """, (limit, offset))
    result = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return result

def void_obs_for_batch(encounter_ids):
    if not encounter_ids:
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        format_strings = ','.join(['%s'] * len(encounter_ids))
        query = f"""
            UPDATE obs 
            SET voided = 1 
            WHERE encounter_id IN ({format_strings})
        """
        cursor.execute(query, encounter_ids)
        conn.commit()
        print(f"Voided {cursor.rowcount} obs rows for batch of {len(encounter_ids)} encounter_ids")
    except Exception as e:
        print(f"Error voiding batch: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def process_batch_range(batch_index):
    offset = batch_index * BATCH_SIZE
    encounter_ids = get_encounter_ids_batch(offset, BATCH_SIZE)
    void_obs_for_batch(encounter_ids)

def main():
    total = get_total_encounters()
    total_batches = math.ceil(total / BATCH_SIZE)
    print(f"Total encounters: {total}, processing in {total_batches} batches...")

    with Pool(cpu_count()) as pool:
        pool.map(process_batch_range, range(total_batches))

    print("All batches processed.")

if __name__ == "__main__":
    main()
