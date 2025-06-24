import mysql.connector
from multiprocessing import Pool, cpu_count
import math
from datetime import date

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

BATCH_SIZE = 1000
TARGET_DATE = date(2025, 6, 24)  # yyyy, m, d

def get_total_rows_for_today():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM obs ob
        INNER JOIN patient_encounter_mapping em 
        ON ob.encounter_id = em.encounter_id
        WHERE DATE(ob.date_created) = %s
    """, (TARGET_DATE,))
    total = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return total

def delete_obs_batch(offset, limit):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        # Select obs_id of rows to delete
        cursor.execute(f"""
            SELECT ob.obs_id
            FROM obs ob
            INNER JOIN patient_encounter_mapping em 
                ON ob.encounter_id = em.encounter_id
            WHERE DATE(ob.date_created) = %s
            ORDER BY ob.obs_id
            LIMIT %s OFFSET %s
        """, (TARGET_DATE, limit, offset))
        obs_ids = [row[0] for row in cursor.fetchall()]

        if obs_ids:
            format_strings = ','.join(['%s'] * len(obs_ids))
            delete_query = f"DELETE FROM obs WHERE obs_id IN ({format_strings})"
            cursor.execute(delete_query, obs_ids)
            conn.commit()
            print(f"Deleted batch of {len(obs_ids)} obs rows at offset {offset}")
    except Exception as e:
        print(f"Error at offset {offset}: {e}")
        conn.rollback()
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        cursor.close()
        conn.close()

def main():
    total = get_total_rows_for_today()
    total_batches = math.ceil(total / BATCH_SIZE)
    print(f"Total obs rows to delete from {TARGET_DATE}: {total}, in {total_batches} batches")

    with Pool(cpu_count()) as pool:
        pool.starmap(delete_obs_batch, [(i * BATCH_SIZE, BATCH_SIZE) for i in range(total_batches)])

    print("Deletion completed.")

if __name__ == "__main__":
    main()
