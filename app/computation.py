import os
import tempfile
import shutil
import requests
import json
import mariadb
import time
import subprocess


from flask import Request
from typing import List, Tuple, Dict

import python_distance
from .config import *


def get_random_pdb_ids(number: int) -> List[str]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    c.execute(f'SELECT gesamtId FROM proteinChain ORDER BY RAND() LIMIT %s', (number,))
    pdb_ids = sorted(row[0].split(':')[0] for row in c.fetchall())
    c.close()
    conn.close()
    return pdb_ids


def get_names(pdb_ids: List[str]) -> Dict[str, str]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    names = {}
    for pdb_id in pdb_ids:
        c.execute(f'SELECT name FROM protein WHERE pdbId = %s', (pdb_id,))
        names[pdb_id] = c.fetchall()[0][0]
    c.close()
    conn.close()
    return names


def search_title(query: str, limit: int) -> List[str]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    c.execute(f'SELECT id FROM proteinId WHERE id IN (SELECT pdbId FROM protein WHERE name LIKE %s) LIMIT %s',
              (f'%{query}%', limit))
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
        raise RuntimeError('No chains having at least 10 residues detected.')

    tmpdir = tempfile.mkdtemp(prefix='query', dir=COMPUTATIONS_DIR)
    os.chmod(tmpdir, 0o755)

    prefix = pdb_id[:2].lower()
    shutil.copy(os.path.join(RAW_PDB_DIR, prefix, f'{pdb_id.lower()}_updated.cif'), os.path.join(tmpdir, 'query'))
    for chain in chains:
        filename = os.path.join(ARCHIVE_DIR, prefix, f'{pdb_id}:{chain}.bin')
        shutil.copy(filename, os.path.join(tmpdir, f'query:{chain}.bin'))

    job_id = os.path.basename(tmpdir)[len('query'):]
    return job_id, chains


def process_input(req: Request) -> Tuple[str, List[str]]:
    tmpdir = tempfile.mkdtemp(prefix='query', dir=COMPUTATIONS_DIR)
    os.chmod(tmpdir, 0o755)
    path = os.path.join(tmpdir, 'query')
    req.files['file'].save(path)

    job_id = os.path.basename(tmpdir)[len('query'):]
    chains = python_distance.save_chains(os.path.join(tmpdir, 'query'), tmpdir, 'query')
    if not chains:
        raise RuntimeError('No chains having at least 10 residues detected.')

    chain_ids, sizes = zip(*chains)
    return job_id, list(chain_ids)


def get_results_messif(query: str, radius: float, num_results: int, req_type: str, job_id: str) -> Tuple[List[str], Dict[str, int]]:
    parameters = {'queryid': query, 'k': num_results, 'job_id': job_id}
    server = 'http://similar-pdb.cerit-sc.cz'
    if req_type == 'sketches_small':
        url = f'{server}:20009/searchsketches'
    elif req_type == 'sketches_large':
        url = f'{server}:20003/searchsketches'
        parameters['radius'] = radius
    else:
        url = f'{server}:20001/search'
        parameters['radius'] = radius

    req = requests.get(url, params=parameters)
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
        'pivot_dist_cached': response['query_record']['CachedPivotsDists'],
        'all_dist_cached': response['query_record']['CachedDistsTotal'],
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


def get_results(query: str, other: str, min_qscore: float) -> Tuple[float, float, float, int, List[float]]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()

    select_query = f'SELECT qscore, rmsd, seqIdentity, alignedResidues, rotationStats FROM queriesNearestNeighboursStats ' \
                   f'WHERE queryGesamtId = %s AND nnGesamtId = %s'
    c.execute(select_query, (query, other))
    query_result = c.fetchall()
    if not query_result:
        begin = time.time()
        _, *res = python_distance.get_results(query, other, ARCHIVE_DIR, min_qscore)
        end = time.time()
        elapsed = int((end - begin) * 1000)
        if elapsed > 500:
            insert_query = f'INSERT IGNORE INTO queriesNearestNeighboursStats VALUES' \
                           f'(%s, NULL, %s, %s, %s, %s, %s, %s, %s)'
            matrix_values = ';'.join(res[4])
            c.execute(insert_query, (elapsed, query, other, res[0], res[1], res[3], res[2], matrix_values))
            conn.commit()
    else:
        matrix = [float(x) for x in query_result[0][-1].split(';')]
        res = [*query_result[0][:-1], matrix]
    c.close()
    conn.close()
    return res


def get_stats(query: str, other: str, min_qscore: float, job_id: str) -> Tuple[float, float, float, int]:
    results = get_results(query, other, min_qscore)
    matrix_T = results[-1]

    directory = os.path.join(COMPUTATIONS_DIR, f'query{job_id}')

    if results[0] > min_qscore:
        try:
            python_distance.prepare_PDB(other, RAW_PDB_DIR, directory, matrix_T)
            query_pdb = os.path.join(directory, 'query.pdb')
            other_pdb = os.path.join(directory, f'{other}.aligned.pdb')
            output_png = os.path.join(directory, f'{other}.aligned.png')
            args = ['pymol', '-qrc', os.path.join(os.path.dirname(__file__), 'draw.pml'), '--', query_pdb, other_pdb, output_png]
        except:
            print('Cannot generate alignment and image')
    return results[:-1]

