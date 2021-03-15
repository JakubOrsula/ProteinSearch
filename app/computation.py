import os
import tempfile
import shutil
import requests
import json
import mariadb
import time

from flask import Request
from typing import List, Tuple, Dict

import python_distance
from .config import *


def get_random_pdb_ids(number: int) -> List[str]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    c.execute(f'SELECT gesamtId FROM proteinChain ORDER BY RAND() LIMIT %s', (number, ))
    pdb_ids = sorted(row[0].split(':')[0] for row in c.fetchall())
    c.close()
    conn.close()
    return pdb_ids


def get_names(pdb_ids: List[str]) -> Dict[str, str]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    names = {}
    for pdb_id in pdb_ids:
        c.execute(f'SELECT name FROM protein WHERE pdbId = %s', (pdb_id, ))
        names[pdb_id] = c.fetchall()[0][0]
    c.close()
    conn.close()
    return names


def search_title(query: str, limit: int) -> List[str]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    c.execute(f'SELECT pdbId FROM protein WHERE name LIKE %s LIMIT %s', (f'%{query}%', limit))
    pdb_ids = sorted(row[0].split(':')[0] for row in c.fetchall())
    c.close()
    conn.close()
    return pdb_ids


def prepare_indexed_chain(pdb_id: str) -> Tuple[str, List[str]]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    c.execute(f'SELECT gesamtId FROM proteinChain WHERE gesamtId LIKE %s', (f'{pdb_id}%',))
    chains = [chain_id[0].split(':')[1] for chain_id in c.fetchall()]
    c.close()
    conn.close()
    if not chains:
        raise RuntimeError('No protein chains detected.')

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
    if response['status']['code'] not in (200, 201):
        print(response)
        raise RuntimeError('MESSIF returned something wrong')

    messif_ids = [int(record['_id']) for record in response['answer_records']]

    statistics = {
        'pivot_dist_time': response['query_record']['pivotDistTimes'],
        'pivot_dist_count': response['query_record']['pivotDistCount'],
        'search_dist_time': response['statistics']['OperationTime'],
        'search_dist_count': response['statistics']['DistanceComputations']
    }

    if not messif_ids:
        return [], statistics

    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    query_template = ', '.join(['%s'] * len(messif_ids))
    c.execute(f'SELECT gesamtId FROM proteinChain WHERE intId IN ({query_template})', tuple(messif_ids))
    chain_ids = [candidate[0] for candidate in c.fetchall()]
    c.close()
    conn.close()

    return chain_ids, statistics


def get_stats(query: str, other: str) -> Tuple[float, float, float, int]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()

    select_query = f'SELECT qscore, rmsd, seqIdentity, alignedResidues FROM queriesNearestNeighboursStats ' \
                   f'WHERE queryGesamtId = %s AND nnGesamtId = %s'
    c.execute(select_query, (query, other))
    query_result = c.fetchall()
    if not query_result:
        python_distance.init_library(ARCHIVE_DIR, '/dev/null', True, 0, 10)
        begin = time.time()
        _, *res = python_distance.get_results(query, other, ARCHIVE_DIR, QSCORE_THRESHOLD)
        end = time.time()
        elapsed = int((end - begin) * 1000)
        if elapsed > 1000:
            insert_query = f'INSERT IGNORE INTO queriesNearestNeighboursStats VALUES' \
                           f'(%s, NULL, %s, %s, %s, %s, %s, %s)'
            c.execute(insert_query, (elapsed, query, other, res[0], res[1], res[3], res[2]))
            conn.commit()
    else:
        res = query_result[0]
    c.close()
    conn.close()
    return res

