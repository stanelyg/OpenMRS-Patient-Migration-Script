import mysql.connector
from multiprocessing import Pool
from functools import partial

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

BATCH_SIZE = 10000  
MAX_PROCESSES = 4  

def get_obs_ids_for_deletion():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT obs_id FROM obs_migration_log WHERE obs_id IS NOT NULL ORDER BY obs_id")
    obs_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return obs_ids

def delete_obs_batch(batch_number, obs_ids):
    if not obs_ids:
        return
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        placeholders = ','.join(['%s'] * len(obs_ids))
        query = f"DELETE FROM obs WHERE obs_id IN ({placeholders})"
        cursor.execute(query, obs_ids)
        conn.commit()
        print(f"[Batch {batch_number}] Deleted {len(obs_ids)} obs records")
    except Exception as e:
        print(f"[Batch {batch_number}] Error: {e}")
        conn.rollback()
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        cursor.close()
        conn.close()

def chunkify(lst, n):
    for i in range(0, len(lst), n):
        yield (i // n + 1, lst[i:i + n])  # batch_number, chunk

def main():
    obs_ids = get_obs_ids_for_deletion()
    total = len(obs_ids)
    print(f"Total obs records to delete: {total}")
    
    batches = list(chunkify(obs_ids, BATCH_SIZE))
    print(f"Running {len(batches)} batches with up to {BATCH_SIZE} per batch")

    with Pool(processes=MAX_PROCESSES) as pool:
        pool.starmap(delete_obs_batch, batches)

    print("Deletion completed.")

if __name__ == "__main__":
    main()
