import argparse
import mariadb
import configparser
import os
import gemmi
import gzip
from pathlib import Path
import python_distance
import shutil
import tqdm
from typing import Optional
from concurrent.futures import as_completed, ProcessPoolExecutor


def get_dir(filename: str) -> str:
    return Path(filename).name[1:3]


def create_necessary_directories(mirror_dir: str, binary_dir: str, raw_dir: str) -> None:
    mirror_dirs = set(os.listdir(mirror_dir))
    binary_dirs = set(os.listdir(binary_dir))
    raw_dirs = set(os.listdir(raw_dir))
    for d in mirror_dirs:
        if d not in binary_dirs:
            os.mkdir(Path(binary_dir) / d)
        if d not in raw_dirs:
            os.mkdir(Path(raw_dir) / d)


def get_whats_updated(mirror_dir: str, raw_dir: str) -> tuple[list[str], list[str], list[str]]:
    mirror_files = set()
    for dirpath, _, fnames in os.walk(Path(mirror_dir)):
        for filename in fnames:
            mirror_files.add(Path(filename).with_suffix('').name)

    raw_files = set()
    for dirpath, _, fnames in os.walk(Path(raw_dir)):
        for filename in fnames:
            raw_files.add(Path(filename).name)

    removed_files = []
    for filename in raw_files:
        if filename not in mirror_files:
            removed_files.append(filename)

    new_files = []
    modified_files = []
    for filename in mirror_files:
        if filename not in raw_files:
            new_files.append(filename)
        else:
            gzip_path = Path(mirror_dir) / get_dir(filename) / f'{filename}.gz'
            raw_path = Path(raw_dir) / get_dir(filename) / filename
            with gzip.open(gzip_path, 'rt') as f_gzip, open(raw_path, 'r') as f_raw:
                if f_gzip.read() != f_raw.read():
                    modified_files.append(filename)

    return new_files, modified_files, removed_files


def remove_chains(files: list[str], conn: mariadb.connection) -> None:
    cursor = conn.cursor()
    pdb_ids = [Path(file).with_suffix('').name.upper() for file in files]
    print(pdb_ids)
    for pdb_id in pdb_ids:
        cursor.execute('SELECT intId FROM proteinChain WHERE gesamtId LIKE %s', (f'{pdb_id}%',))
        int_ids = [res[0] for res in cursor.fetchall()]

        # cursor.execute(f'SELECT * FROM proteinChain WHERE intId IN ({})')
        print(int_ids)

        # cursor.execute('DELETE FROM proteinChain WHERE gesamtId LIKE %s', (f'{pdb_id}%',))
        # cursor.execute('DELETE FROM protein WHERE pdbId = %s', (pdb_id,))

    # conn.commit()
    cursor.close()


def decompress_file(filename, src_dir: str, dest_dir: str) -> None:
    with gzip.open(Path(src_dir) / get_dir(filename) / f'{filename}.gz', 'rt') as f_in:
        with open(Path(dest_dir) / get_dir(filename) / filename, 'w') as f_out:
            shutil.copyfileobj(f_in, f_out)


def create_binaries(filename: str, src_dir: str, dest_dir: str) -> list[tuple[str, str, int]]:
    file = Path(src_dir) / get_dir(filename) / filename
    dirname = get_dir(filename)
    pdb_id = file.name[:4].upper()
    results = python_distance.save_chains(str(file), str(Path(dest_dir) / dirname), pdb_id)
    return [(filename, f'{pdb_id}:{chain_id}', size) for chain_id, size in results]


def read_protein_title(filename: str) -> tuple[str, Optional[str]]:
    doc = gemmi.cif.read(filename)
    block = doc.sole_block()
    pdb_id = block.find_pair('_struct.entry_id')
    pdb_title = block.find_pair('_struct.title')

    if pdb_id is None:
        pdb_id = os.path.basename(filename)[:4].upper()
        return pdb_id, None

    if pdb_title is None:
        return pdb_id, None

    pdb_id = pdb_id[1]
    delimiter = pdb_title[1][0]
    title = pdb_title[1].strip(delimiter).strip()
    return pdb_id, title


def add_chains(files: list[str], mirror_dir: str, raw_dir: str, binary_dir: str, conn: mariadb.connection,
               executor: ProcessPoolExecutor) -> None:

    cursor = conn.cursor()

    # Decompress gzipped CIFs
    jobs = [executor.submit(decompress_file, filename, mirror_dir, raw_dir) for filename in files]
    for _ in tqdm.tqdm(as_completed(jobs), total=len(jobs), desc='Decompressing files'):
        pass

    # Make binary chain representations
    jobs = [executor.submit(create_binaries, filename, raw_dir, binary_dir) for filename in files]
    converted_chains = []
    for job in tqdm.tqdm(as_completed(jobs), total=len(jobs), desc='Converting chains to binaries'):
        for filename, chain_id, size in job.result():
            converted_chains.append((filename, chain_id, size))

    # Get names of the proteins
    converted_proteins_filenames = {str(Path(raw_dir) / get_dir(filename) / filename) for filename, *_ in converted_chains}
    jobs = [executor.submit(read_protein_title, filename) for filename in converted_proteins_filenames]
    titles = []
    for job in tqdm.tqdm(as_completed(jobs), total=len(jobs), desc='Reading protein names'):
        pdb_id, title = job.result()
        if title is None:
            print(f'Error: No title found for: {pdb_id}\n')
        titles.append((pdb_id, title))

    # Store names of the proteins
    insert_query = 'INSERT IGNORE INTO protein VALUES (%s, %s)'
    if titles:
        cursor.executemany(insert_query, titles)
        conn.commit()

    chains_to_store = [(chain_id, size) for _, chain_id, size in converted_chains]
    insert_query = 'INSERT IGNORE INTO proteinChain (gesamtId, chainLength) VALUES (%s, %s)'
    if chains_to_store:
        cursor.executemany(insert_query, chains_to_store)
        conn.commit()

    # TODO add chains to proteinChainMetadata?

    cursor.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='/etc/protein_search.ini', help='File with configuration of DB')
    parser.add_argument('--mirror-directory', type=str, default=True, help='Directory with rsynced files')
    parser.add_argument('--binary-directory', type=str, required=True, help='Directory to store binaries')
    parser.add_argument('--raw-directory', type=str, required=True, help='Directory with uncompressed files')
    parser.add_argument('--workers', type=int, default=1, help='Number of workers ')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    executor = ProcessPoolExecutor(args.workers)

    conn = mariadb.connect(host=config['db']['host'], user=config['db']['user'], password=config['db']['password'],
                           database=config['db']['database'])
    print('*** Updating directories ***')
    create_necessary_directories(args.mirror_directory, args.binary_directory, args.raw_directory)

    print('*** Looking for modifications ***')
    new_files, modified_files, removed_files = get_whats_updated(args.mirror_directory, args.raw_directory)

    print('*** Processing new entries ***')
    add_chains(new_files, args.mirror_directory, args.raw_directory, args.binary_directory, conn, executor)

    print('*** Removing obsoleted entries ***')
    remove_chains(removed_files, conn)
    print('*** Updating modified entries (1 - remove) ***')

    print('*** Updating modified entries (2 - add) ***')

    conn.close()


if __name__ == '__main__':
    main()
