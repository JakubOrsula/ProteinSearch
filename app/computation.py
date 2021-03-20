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
    shutil.copy(os.path.join(RAW_PDB_DIR, f'{pdb_id.lower()}.cif'), os.path.join(tmpdir, 'query'))
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


def get_results_messif(query: str, radius: float, num_results: int, req_type: str, job_id: str) \
        -> Tuple[List[str], Dict[str, int]]:
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
        'pivotDistCountTotal': 0,
        'pivotDistCountCached': 0,
        'pivotTime': 0,
        'searchDistCountTotal': 0,
        'searchDistCountCached': 0,
        'searchTime': 0,
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


def get_similarity_results(query: str, other: str, min_qscore: float) -> Tuple[float, float, float, int, List[float]]:
    conn = mariadb.connect(user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()

    select_query = (f'SELECT qscore, rmsd, seqIdentity, alignedResidues, rotationStats '
                    f'FROM queriesNearestNeighboursStats WHERE queryGesamtId = %s AND nnGesamtId = %s')
    c.execute(select_query, (query, other))
    query_result = c.fetchall()
    if not query_result:
        begin = time.time()
        _, qscore, rmsd, seq_identity, aligned, T = python_distance.get_results(query, other, ARCHIVE_DIR, min_qscore)
        end = time.time()
        elapsed = int((end - begin) * 1000)
        results = (qscore, rmsd, seq_identity, aligned, T)
        if elapsed > 500:
            insert_query = (f'INSERT IGNORE INTO queriesNearestNeighboursStats '
                            f'(evaluationTime, queryGesamtId, nnGesamtId,'
                            f' qscore, rmsd, alignedResidues, seqIdentity, rotationStats) '
                            f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)')
            T_str = ';'.join(f'{x:.3f}' for x in T)
            c.execute(insert_query, (elapsed, query, other, qscore, rmsd, aligned, seq_identity, T_str))
            conn.commit()
    else:
        qscore, rmsd, seq_identity, aligned, T = query_result[0]
        T = [float(x) for x in query_result[-1].split(';')]
        results = float(qscore), float(rmsd), float(seq_identity), int(aligned), T
    c.close()
    conn.close()
    return results


def get_stats(query: str, other: str, min_qscore: float, job_id: str) -> Tuple[float, float, float, int]:
    qscore, rmsd, seq_identity, aligned, T = get_similarity_results(query, other, min_qscore)
    directory = os.path.join(COMPUTATIONS_DIR, f'query{job_id}')
    if qscore > min_qscore:
        try:
            python_distance.prepare_PDB(other, RAW_PDB_DIR, directory, T)
            query_pdb = os.path.join(directory, 'query.pdb')
            other_pdb = os.path.join(directory, f'{other}.aligned.pdb')
            output_png = os.path.join(directory, f'{other}.aligned.png')
            args = ['pymol', '-qrc', os.path.join(os.path.dirname(__file__), 'draw.pml'), '--', query_pdb, other_pdb,
                    output_png]
            subprocess.run(args)
        except:
            print('Cannot generate alignment and image')
    return qscore, rmsd, seq_identity, aligned
