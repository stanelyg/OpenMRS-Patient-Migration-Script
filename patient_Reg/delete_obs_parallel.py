import mysql.connector
from multiprocessing import Pool
import time

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

def get_obs_ids_to_delete():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT obs_id FROM obs WHERE  concept_id IN (1001782,1001775)
        ORDER BY obs_id
    """)
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
                cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_obs_ids")
                cursor.execute("CREATE TEMPORARY TABLE temp_obs_ids (obs_id INT PRIMARY KEY)")
                cursor.executemany("INSERT INTO temp_obs_ids (obs_id) VALUES (%s)", [(obs_id,) for obs_id in batch])

                cursor.execute("""
                    DELETE m FROM obs m
                    JOIN temp_obs_ids t ON m.obs_id = t.obs_id
                """)
                cursor.execute("""
                    DELETE lg FROM obs_biomedical_migration_log lg
                    JOIN temp_obs_ids t ON lg.obs_id = t.obs_id
                """)
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
    k, m = divmod(len(lst), n_parts)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n_parts)]

def main():
    obs_ids = get_obs_ids_to_delete()
    total = len(obs_ids)
    print(f"Total obs records to delete: {total}")

    if total == 0:
        print("No obs records to delete.")
        return

    partitions = split_list(obs_ids, MAX_PROCESSES)
    print(f"Spawning {len(partitions)} processes")

    with Pool(processes=MAX_PROCESSES) as pool:
        pool.starmap(delete_obs_in_batches, [(partition, i + 1) for i, partition in enumerate(partitions)])

    print("Deletion completed.")

if __name__ == "__main__":
    main()
