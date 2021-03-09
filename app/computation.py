import os
import tempfile
import multiprocessing.pool
import shutil
import requests
import json
import mariadb
import time

from flask import Request
from typing import List, Tuple, Dict

import python_distance
from .config import *


def prepare_indexed_chain(req: Request):
    pdb_id = req.form['pdbid'].upper()

    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    c.execute(f'SELECT gesamtId FROM proteinChain WHERE gesamtId LIKE "{pdb_id}%"')
    chains = [chain_id[0].split(':')[1] for chain_id in c.fetchall()]
    c.close()
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
    chains = python_distance.save_chains(os.path.join(tmpdir, 'query'), tmpdir, 'query')
    chain_ids, sizes = zip(*chains)
    return comp_id, list(chain_ids)


def get_results_messif(query: str, radius: float, num_results: int, req_type: str) -> Tuple[List[str], Dict[str, int]]:
    if req_type == 'sketches_small':
        url = f'http://147.251.21.141:20009/searchsketches?queryid={query}&k={num_results}'
    elif req_type == 'sketches_large':
        url = f'http://147.251.21.141:20003/searchsketches?queryid={query}&range={radius}&k={num_results}'
    else:
        url = f'http://147.251.21.141:20001/search?queryid={query}&range={radius}&k={num_results}'

    req = requests.get(url)
    if req.status_code != 200:
        raise RuntimeError('MESSIF not responding')

    response = json.loads(req.content.decode('utf-8'))
    if response['status']['code'] != 200:
        raise RuntimeError('MESSIF returned something wrong')

    messif_ids = ', '.join(record['_id'] for record in response['answer_records'])

    statistics = {
        'pivot_dist_time': response['query_record']['pivotDistTimes'],
        'pivot_dist_count': response['query_record']['pivotDistCount'],
        'search_dist_time': response['statistics']['OperationTime'],
        'search_dist_count': response['statistics']['DistanceComputations']
    }

    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    c.execute(f'SELECT gesamtId FROM proteinChain WHERE intId IN ({messif_ids})')
    chain_ids = [candidate[0] for candidate in c.fetchall()]
    c.close()
    conn.close()

    return chain_ids, statistics


def get_stats(query: str, other: str) -> Tuple[float, float, float, int]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()

    select_query = f'SELECT qscore, rmsd, seqIdentity, alignedResidues FROM queriesNearestNeighboursStats ' \
                   f'WHERE queryGesamtId = "{query}" AND nnGesamtId = "{other}"'
    c.execute(select_query)
    query_result = c.fetchall()
    if not query_result:
        python_distance.init_library(ARCHIVE_DIR, '/dev/null', True, 0, 10)
        begin = time.time()
        _, *res = python_distance.get_results(query, other, ARCHIVE_DIR, QSCORE_THRESHOLD)
        end = time.time()
        elapsed = int((end - begin) * 1000)
        insert_query = f'INSERT IGNORE INTO queriesNearestNeighboursStats VALUES' \
                       f'({elapsed}, NULL, "{query}", "{other}", {res[0]}, {res[1]}, {res[3]}, {res[2]})'
        c.execute(insert_query)

        conn.commit()
    else:
        res = query_result[0]
    c.close()
    conn.close()
    return res


def start_computation(query, radius: float, num_results: int, pool: multiprocessing.Pool) -> Dict[str, multiprocessing.pool.AsyncResult]:
    results = {
        'sketches_small': pool.apply_async(get_results_messif, args=(query, -1, num_results, 'sketches_small')),
        'sketches_large': pool.apply_async(get_results_messif, args=(query, radius, num_results, 'sketches_large')),
        'full': pool.apply_async(get_results_messif, args=(query, radius, num_results, 'full'))
    }
    return results
