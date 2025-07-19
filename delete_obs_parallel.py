import mysql.connector
from multiprocessing import Pool
import time
import math

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'test',
    'database': 'openmrs'
}

BATCH_SIZE = 500
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

def delete_obs_in_batches(obs_ids_partition, process_num):
    total = len(obs_ids_partition)
    print(f"[Process {process_num}] Starting deletion of {total} obs records in batches of {BATCH_SIZE}")

    for batch_index in range(0, total, BATCH_SIZE):
        batch = obs_ids_partition[batch_index: batch_index + BATCH_SIZE]
        attempt = 0

        while attempt < MAX_RETRIES:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            try:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                placeholders = ','.join(['%s'] * len(batch))
                query = f"DELETE FROM obs WHERE obs_id IN ({placeholders})"
                cursor.execute(query, batch)
                conn.commit()
                print(f"[Process {process_num} - Batch {batch_index // BATCH_SIZE + 1}] Deleted {len(batch)} obs records")
                break
            except mysql.connector.Error as e:
                if e.errno == 1205:
                    attempt += 1
                    print(f"[Process {process_num} - Batch {batch_index // BATCH_SIZE + 1}] Lock timeout. Retry {attempt}/{MAX_RETRIES} in {RETRY_DELAY}s...")
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"[Process {process_num} - Batch {batch_index // BATCH_SIZE + 1}] Error: {e}")
                    break
            finally:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                cursor.close()
                conn.close()

def split_list(lst, n_parts):
    """Splits a list into n roughly equal parts."""
    k, m = divmod(len(lst), n_parts)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n_parts)]

def truncate_obs_migration_log():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE obs_migration_log")
        conn.commit()
        print("obs_migration_log table truncated.")
    except mysql.connector.Error as e:
        print(f"Error truncating obs_migration_log: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    obs_ids = get_obs_ids_for_deletion()
    total = len(obs_ids)
    print(f"Total obs records to delete: {total}")

    if total == 0:
        print("No obs records to delete.")
        return

    partitions = split_list(obs_ids, MAX_PROCESSES)

    print(f"Spawning {len(partitions)} processes")

    with Pool(processes=MAX_PROCESSES) as pool:
        pool.starmap(delete_obs_in_batches, [(partition, i + 1) for i, partition in enumerate(partitions)])

    truncate_obs_migration_log()

    print("Deletion completed and obs_migration_log cleared.")

if __name__ == "__main__":
    main()
