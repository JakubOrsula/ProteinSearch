import os
import subprocess
import multiprocessing
import python_distance
import itertools
import tempfile
import functools
import shutil
import filecmp

from typing import List, Tuple

REMOTE_DIR = 'rsync.ebi.ac.uk::pub/databases/rcsb/pdb-remediated/data/structures/divided/mmCIF/10'
PORT = 873
# REMOTE_DIR = '/tmp/dir1/'
LOCAL_DIR = '/tmp/pdbe_sync/'
BINARY_DIR = '/tmp/binary'


def pdb_rsync():
    args = ['rsync', '-rlptazi', '--delete', f'--port={PORT}', REMOTE_DIR, LOCAL_DIR]
    #args = ['rsync', '-ia', '--delete', REMOTE_DIR, LOCAL_DIR]
    run = subprocess.run(args, stdout=subprocess.PIPE)
    stdout = run.stdout.decode('utf-8')
    print(stdout)
    new_directories = []
    new_files = []
    updated_files = []
    deleted_files = []

    for line in stdout.splitlines():
        flags, filename = line.strip().split()
        if flags == '>f+++++++++':
            new_files.append(filename)
        elif flags.startswith('>f'):
            updated_files.append(filename)
        elif flags.startswith('cd'):
            new_directories.append(filename)
        elif flags.startswith('*deleting'):
            deleted_files.append(filename)

    return new_directories, new_files, updated_files, deleted_files


def save_chains(filename: str, output_dir: str) -> List[Tuple[str, int]]:
    basename = os.path.basename(filename)
    pdb_id = basename.split('.')[0].upper()
    chains = python_distance.save_chains(os.path.join(LOCAL_DIR, filename), output_dir, pdb_id)
    return [(f'{pdb_id}:{chain_id}', size) for chain_id, size in chains]


def create_new_directories(dir_names: List[str]) -> None:
    for directory in dir_names:
        full_path = os.path.join(BINARY_DIR, directory)
        try:
            os.mkdir(full_path)
            print(f'Created directory: {full_path}')
        except FileExistsError:
            print(f'WARNING: Tried to create existing directory: {full_path}')


def process_new_files(filenames: List[str]) -> List[Tuple[str, int]]:
    with tempfile.TemporaryDirectory(prefix='save_chains') as tmpdir:
        with multiprocessing.Pool() as pool:
            results = pool.map(functools.partial(save_chains, output_dir=tmpdir), filenames)

        results = list(itertools.chain.from_iterable(results))
        for chain_id, _ in results:
            subdir = chain_id[1:3].lower()
            shutil.copyfile(os.path.join(tmpdir, f'{chain_id}.bin'),
                            os.path.join(BINARY_DIR, subdir, f'{chain_id}.bin'))

    return results


def process_updated_files(filenames: List[str]) -> List[Tuple[str, int]]:
    results = []
    with tempfile.TemporaryDirectory(prefix='save_chains') as tmpdir:
        with multiprocessing.Pool() as pool:
            processed_chains = pool.map(functools.partial(save_chains, output_dir=tmpdir), filenames)

        processed_chains = list(itertools.chain.from_iterable(processed_chains))
        for chain_id, size in processed_chains:
            subdir = chain_id[1:3].lower()
            if not filecmp.cmp(os.path.join(tmpdir, f'{chain_id}.bin'),
                               os.path.join(BINARY_DIR, subdir, f'{chain_id}.bin')):
                shutil.copyfile(os.path.join(tmpdir, f'{chain_id}.bin'),
                                os.path.join(BINARY_DIR, subdir, f'{chain_id}.bin'))
                results.append((chain_id, size))

    return results


def process_deleted_files(filenames: List[str]) -> List[str]:
    deleted = []
    for filename in filenames:
        basename = os.path.basename(filename)
        pdbid = basename[:4].upper()
        dirname = os.path.dirname(filename)

        for file in os.listdir(os.path.join(BINARY_DIR, dirname)):
            if file.startswith(pdbid):
                deleted.append(os.path.splitext(file)[0])
                os.remove(os.path.join(BINARY_DIR, dirname, file))

    return deleted


def full_update():
    new_directories, new_files, updated_files, deleted_files = pdb_rsync()

    create_new_directories(new_directories)

    new_chains = process_new_files(new_files)
    print('New chains')
    print(new_chains)

    updated_chains = process_updated_files(updated_files)
    print('Updated chains')
    print(updated_chains)

    deleted_chains = process_deleted_files(deleted_files)
    print('Deleted chains')
    print(deleted_chains)


if __name__ == '__main__':
    full_update()
