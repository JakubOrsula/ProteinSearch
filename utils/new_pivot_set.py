import argparse
import configparser
import mariadb
import random
from pathlib import Path
import shutil

PIVOT_SIZE_LIMIT = 2500


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='/etc/protein_search.ini', help='File with configuration of DB')
    parser.add_argument('--binary-directory', type=str, required=True, help='Directory to store binaries')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    conn = mariadb.connect(host=config['db']['host'], user=config['db']['user'], password=config['db']['password'],
                           database=config['db']['database'])

    cursor = conn.cursor()

    cursor.execute('SELECT gesamtId, chainLength from proteinChain WHERE indexedAsDataObject=1')
    data = cursor.fetchall()
    data.sort(key=lambda x: x[1])
    print(f"got {len(data)} chains")

    pivots = []
    chunk_size = len(data) // 512
    data = [x for x in data if x[1] <= PIVOT_SIZE_LIMIT]
    for i in range(512):
        pivots.append(random.choice(data[i * chunk_size: (i + 1) * chunk_size]))

    cursor.execute('INSERT INTO pivotSet VALUES ()')

    cursor.execute('SELECT MAX(id) FROM pivotSet')
    pivot_set_id = cursor.fetchall()[0][0]

    pivot_entries = [(f'@{pivot_set_id}_{pivot[0]}', pivot[1], 0) for pivot in pivots]

    cursor.executemany(
        'INSERT IGNORE INTO proteinChain (gesamtId, chainLength, indexedAsDataObject) VALUES (%s, %s, %s)',
        pivot_entries)

    cursor.execute(f'SELECT intId FROM proteinChain WHERE gesamtId LIKE \'@{pivot_set_id}_%\'')
    ids = [(x[0], pivot_set_id) for x in cursor.fetchall()]

    print(f"inserting {len(ids)} pivots")
    print(ids[0])
    cursor.executemany('INSERT INTO pivot512 (chainIntId, pivotSetId) VALUES (%s, %s)', ids)

    cursor.execute('UPDATE pivotSet SET currentlyUsed = 0 WHERE currentlyUsed = 1')
    cursor.execute(f'UPDATE pivotSet SET currentlyUsed = 1 WHERE id = {pivot_set_id}')

    conn.commit()

    cursor.close()
    conn.close()

    (Path(args.binary_directory) / 'pivots' / str(pivot_set_id)).mkdir(parents=True, exist_ok=True)

    for chain_id, _ in pivots:
        shutil.copyfile(Path(args.binary_directory) / chain_id[1:3].lower() / f'{chain_id}.bin',
                        Path(args.binary_directory) / 'pivots' / str(pivot_set_id) / f'{chain_id}.bin')


if __name__ == '__main__':
    main()
