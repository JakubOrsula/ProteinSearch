import python_distance
import configparser
import mariadb
import gemmi
from concurrent.futures import as_completed, ProcessPoolExecutor
import argparse
import sys
import os
from typing import List, Optional, Tuple
import tqdm


def create_binaries(filename: str, output_dir: str) -> List[Tuple[str, str, int]]:
    basename = os.path.basename(filename)
    dirname = basename[:2]
    pdb_id = basename[:4].upper()
    results = python_distance.save_chains(filename, os.path.join(output_dir, dirname), pdb_id)
    return [(filename, f'{pdb_id}:{chain_id}', size) for chain_id, size in results]


def read_protein_title(filename: str) -> Tuple[str, Optional[str]]:
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='/etc/protein_search.ini', help='File with configuration of DB')
    parser.add_argument('--list-of-files', type=str, default=None, help='List of files to process')
    parser.add_argument('--refresh', action='store_true', default=False, help='Update even if in DB')
    parser.add_argument('--in-directory', type=str, default=None, help='Directory to process')
    parser.add_argument('--out-directory', type=str, required=True, help='Directory to store binaries')
    parser.add_argument('--log', type=str, required=True, help='File to store log with sizes of chains')
    parser.add_argument('--workers', type=int, default=1, help='Number of workers ')
    args = parser.parse_args()

    if (args.list_of_files is None and args.in_directory is None) or \
            (args.list_of_files is not None and args.in_directory is not None):
        print('Exactly one of --list-of-files or --in-directory arguments must be present', file=sys.stderr)
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(args.config)

    conn = mariadb.connect(host=config['db']['host'], user=config['db']['user'], password=config['db']['password'],
                           database=config['db']['database'])
    c = conn.cursor()

    # Load proteins already stored in DB
    processed_proteins = {}
    if not args.refresh:
        query = 'SELECT gesamtID FROM proteinChain'
        c.execute(query)
        data = c.fetchall()
        processed_proteins = {chain_id[0][:4].lower() for chain_id in data}

    # Get a list of protein files to process
    filenames = []
    if args.list_of_files is not None:
        with open(args.list_of_file) as f:
            for line in f:
                filenames.append(line.strip())
    else:
        for dirpath, _, fnames in os.walk(args.in_directory):
            for filename in fnames:
                filenames.append(os.path.join(dirpath, filename))

    # Create directories if necessary
    dirnames = {os.path.basename(filename)[:2] for filename in filenames}
    for dirname in dirnames:
        try:
            os.mkdir(os.path.join(args.out_directory, dirname))
        except FileExistsError:
            pass

    # Exclude proteins already stored in DB
    to_process = [filename for filename in filenames if os.path.basename(filename)[:4] not in processed_proteins]

    f = open(args.log, 'w')
    executor = ProcessPoolExecutor(args.workers)

    # Create binaries
    jobs = [executor.submit(create_binaries, filename, output_dir=args.out_directory) for filename in to_process]
    converted_chains = []
    for job in tqdm.tqdm(as_completed(jobs), total=len(jobs), desc='Converting chains to binaries'):
        for filename, chain_id, size in job.result():
            converted_chains.append((filename, chain_id, size))
            f.write(f'Converted: {chain_id} of size {size} from file {filename}\n')

    # Save names of the converted proteins
    converted_proteins_filenames = {filename for filename, *_ in converted_chains}
    jobs = [executor.submit(read_protein_title, filename) for filename in converted_proteins_filenames]
    titles = []
    for job in tqdm.tqdm(as_completed(jobs), total=len(jobs), desc='Reading protein names'):
        pdb_id, title = job.result()
        if title is None:
            f.write(f'Error: No title found for: {pdb_id}\n')
        titles.append((pdb_id, title))
        f.write(f'Read title: {pdb_id}\n')

    print('Storing protein titles into the DB')
    insert_query = 'INSERT IGNORE INTO protein VALUES (%s, %s)'
    c.executemany(insert_query, titles)
    conn.commit()

    # Save chains into DB
    print('Storing chains into the DB')
    chains_to_store = [(chain_id, size) for _, chain_id, size in converted_chains]
    insert_query = 'INSERT IGNORE INTO proteinChain (gesamtId, chainLength) VALUES (%s, %s)'
    c.executemany(insert_query, chains_to_store)
    conn.commit()

    c.close()
    conn.close()

    f.close()


if __name__ == '__main__':
    main()
