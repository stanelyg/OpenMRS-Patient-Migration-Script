import mysql.connector
from multiprocessing import Pool
from functools import partial
import time

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

BATCH_SIZE = 10000
MAX_PROCESSES = 3
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def get_obs_ids_for_deletion():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT obs_id FROM obs_migration_log WHERE obs_id IS NOT NULL ORDER BY obs_id")
    obs_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return obs_ids

def delete_obs_batch(batch_number, obs_ids):
    attempt = 0
    while attempt < MAX_RETRIES:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            placeholders = ','.join(['%s'] * len(obs_ids))
            query = f"DELETE FROM obs WHERE obs_id IN ({placeholders})"
            cursor.execute(query, obs_ids)
            conn.commit()
            print(f"[Batch {batch_number}] Deleted {len(obs_ids)} obs records")
            break  # Success
        except mysql.connector.Error as e:
            if e.errno == 1205:
                attempt += 1
                print(f"[Batch {batch_number}] Lock timeout. Retry {attempt}/{MAX_RETRIES} in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"[Batch {batch_number}] Error: {e}")
                break
        finally:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            cursor.close()
            conn.close()

def chunkify(lst, n):
    for i in range(0, len(lst), n):
        yield (i // n + 1, lst[i:i + n])

def main():
    obs_ids = get_obs_ids_for_deletion()
    total = len(obs_ids)
    print(f"Total obs records to delete: {total}")
    
    batches = list(chunkify(obs_ids, BATCH_SIZE))
    print(f"Running {len(batches)} batches with {MAX_PROCESSES} processes")

    with Pool(processes=MAX_PROCESSES) as pool:
        pool.starmap(delete_obs_batch, batches)

    print("Deletion completed.")

if __name__ == "__main__":
    main()
