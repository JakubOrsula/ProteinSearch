import os
import subprocess
import tempfile
import multiprocessing.pool
import sqlite3
import shutil
import requests
import json

from flask import Request
from typing import List, Tuple, Dict, Optional

import python_distance
from .config import ARCHIVE_DIR, COMPUTATIONS_DIR, QSCORE_THRESHOLD, RAW_PDB_DIR

db_file = os.path.join(os.path.dirname(__file__), 'chain_ids.db')


def prepare_indexed_chain(req: Request):
    pdb_id = req.form['pdbid'].upper()

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute(f'SELECT chain_id FROM chain_ids WHERE chain_id LIKE "{pdb_id}%"')
    chains = [chain_id[0].split(':')[1] for chain_id in c.fetchall()]
    conn.close()

    if not chains:
        raise RuntimeError()

    tmpdir = tempfile.mkdtemp(prefix='query', dir=COMPUTATIONS_DIR)
    os.chmod(tmpdir, 0o755)

    prefix = pdb_id[:2].lower()
    shutil.copy(os.path.join(RAW_PDB_DIR, f'{pdb_id.lower()}.cif'), os.path.join(tmpdir, 'query'))
    for chain in chains:
        filename = os.path.join(ARCHIVE_DIR, prefix, f'{pdb_id}:{chain}.bin')
        shutil.copy(filename, os.path.join(tmpdir, f'query:{chain}.bin'))

    comp_id = os.path.basename(tmpdir)[len('query'):]
    return comp_id, chains


def process_input(req: Request) -> Tuple[str, List[str]]:
    tmpdir = tempfile.mkdtemp(prefix='query', dir=COMPUTATIONS_DIR)
    os.chmod(tmpdir, 0o755)
    path = os.path.join(tmpdir, 'query')
    req.files['file'].save(path)

    comp_id = os.path.basename(tmpdir)[len('query'):]
    return comp_id, python_distance.save_chains(os.path.join(tmpdir, 'query'), tmpdir)


def get_candidates_messif(query: str, radius: float, num_results: int) -> List[str]:
    url = f'http://147.251.21.141:20001/search?queryid={query}&range={radius}&k={num_results}'
    req = requests.get(url)
    response = json.loads(req.content.decode('utf-8'))
    if response['status']['code'] != 200:
        raise RuntimeError('MESSIF returned something wrong')

    messif_ids = ', '.join(record['_id'] for record in response['answer_records'])

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute(f'SELECT chain_id FROM chain_ids WHERE messif_id IN ({messif_ids});')

    candidates = [candidate[0] for candidate in c.fetchall()]
    conn.close()
    return candidates


def compute_distance(comp_id: str, chain: str, candidate: str) -> None:
    python_distance.init_library(ARCHIVE_DIR, '/dev/null', True, 0, 10)
    res = python_distance.get_results(f'_{comp_id}:{chain}', candidate, ARCHIVE_DIR, QSCORE_THRESHOLD)
    return res


def start_computation(comp_id: str, chain: str, pdb_id: Optional[str], radius: float, num_results: int,
                      pool: multiprocessing.Pool) -> Dict[str, multiprocessing.pool.AsyncResult]:

    if pdb_id is None:
        query = f'_{comp_id}:{chain}'
    else:
        query = f'{pdb_id}:{chain}'

    candidates = get_candidates_messif(query, radius, num_results)
    results = {}
    for candidate in candidates:
        results[candidate] = pool.apply_async(compute_distance, args=(comp_id, chain, candidate))

    return results
