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

CONCEPT_IDS = [
    1354, 159635, 162053, 164951, 166091, 167131, 1000636, 1000645, 1000658,
    1000659, 1000660, 1000661, 1000662, 1000663, 1000664, 1000665, 1000667,
    1000668, 1000669, 1000670, 1000671, 1000672, 1000673, 1000674, 1000675,
    1000677, 1000679, 1000680, 1000685, 1000686, 1000691, 1000692, 1000698,
    1000699, 1000700, 1000705, 1000706, 1000707, 1000708, 1000709, 1000710,
    1000711, 1000712, 1000715, 1000720, 1000735, 1000737, 1000743, 1000744,
    1000745, 1000750, 1000752, 1000756, 1000757, 1000761, 1000763, 1000764,
    1000774, 1000775, 1000784, 1000785, 1000787, 1000788, 1000792, 1000793,
    1000794, 1000795, 1000796, 1000797, 1000798, 1000800, 1000801, 1000802,
    1000803, 1000804, 1000805, 1000806, 1000807, 1000809, 1000810, 1000817,
    1000819, 1000820, 1000822, 1000824, 1000825, 1000826, 1000827, 1000828,
    1000832, 1000833, 1000834, 1000835, 1000836, 1000837, 1000838, 1000839,
    1000840, 1000841, 1000842, 1000843, 1000852, 1000854, 1000855, 1000861,
    1000862, 1000868, 1000870, 1000878, 1001091, 1001343, 1001707, 1001708,
    1001709, 1001711, 1001712, 1001713, 1001714, 1001715, 1001716, 1001717,
    1001718, 1001719, 1001720, 1001721, 1001722, 1001723, 1001724, 1001725,
    1001726, 1001727, 1001728, 1001770, 1001771
]

def get_obs_ids_to_delete():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    placeholders = ','.join(['%s'] * len(CONCEPT_IDS))
    query = f"""
        SELECT obs_id FROM obs 
        WHERE concept_id IN ({placeholders}) 
        AND YEAR(date_created) = 2025
        ORDER BY obs_id
    """
    cursor.execute(query, CONCEPT_IDS)
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
                if e.errno == 1205:  # Lock timeout
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
