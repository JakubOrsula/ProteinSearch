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
    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    c.execute(f'SELECT gesamtId FROM proteinChain ORDER BY RAND() LIMIT %s', (number,))
    pdb_ids = sorted(row[0].split(':')[0] for row in c.fetchall())
    c.close()
    conn.close()
    return pdb_ids


def get_names(pdb_ids: List[str]) -> Dict[str, str]:
    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    names = {}
    for pdb_id in pdb_ids:
        c.execute(f'SELECT name FROM protein WHERE pdbId = %s', (pdb_id,))
        names[pdb_id] = c.fetchall()[0][0]
    c.close()
    conn.close()
    return names


def search_title(query: str, limit: int) -> List[str]:
    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    words = ' '.join(f'+{word}' for word in query.split())
    sql_query = (f'SELECT id FROM proteinId WHERE id IN '
                 f'(SELECT pdbId FROM protein WHERE MATCH(name) AGAINST (%s IN BOOLEAN MODE)) '
                 f'LIMIT %s')

    c.execute(sql_query, (words, limit))
    pdb_ids = sorted(row[0].split(':')[0] for row in c.fetchall())
    c.close()
    conn.close()
    return pdb_ids


def prepare_indexed_chain(pdb_id: str) -> Tuple[str, List[Tuple[str, int]]]:
    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    c.execute(f'SELECT gesamtId, chainLength FROM proteinChain WHERE gesamtId LIKE %s', (f'{pdb_id}%',))
    chains = [(chain_data[0].split(':')[1], chain_data[1]) for chain_data in c.fetchall()]
    c.close()
    conn.close()
    if not chains:
        raise RuntimeError('No chains having at least 10 residues detected.')

    tmpdir = tempfile.mkdtemp(prefix='query', dir=COMPUTATIONS_DIR)
    os.chmod(tmpdir, 0o755)

    prefix = pdb_id[:2].lower()
    shutil.copy(os.path.join(RAW_PDB_DIR, f'{pdb_id.lower()}.cif'), os.path.join(tmpdir, 'query'))
    for chain in (chain[0] for chain in chains):
        filename = os.path.join(ARCHIVE_DIR, prefix, f'{pdb_id}:{chain}.bin')
        shutil.copy(filename, os.path.join(tmpdir, f'query:{chain}.bin'))

    job_id = os.path.basename(tmpdir)[len('query'):]
    return job_id, chains


def process_input(req: Request) -> Tuple[str, List[Tuple[str, int]]]:
    tmpdir = tempfile.mkdtemp(prefix='query', dir=COMPUTATIONS_DIR)
    os.chmod(tmpdir, 0o755)
    path = os.path.join(tmpdir, 'query')
    req.files['file'].save(path)

    job_id = os.path.basename(tmpdir)[len('query'):]
    chains = python_distance.save_chains(os.path.join(tmpdir, 'query'), tmpdir, 'query')
    if not chains:
        raise RuntimeError('No chains having at least 10 residues detected.')

    return job_id, chains


def get_results_messif(query: str, radius: float, num_results: int, phase: str, job_id: str) \
        -> Tuple[List[str], Dict[str, int]]:
    parameters = {'queryid': query, 'k': num_results, 'job_id': job_id}

    if phase in ('sketches_large', 'full'):
        parameters['radius'] = radius

    url = f'http://similar-pdb.cerit-sc.cz:{PORTS[phase]}/search'
    try:
        req = requests.get(url, params=parameters)
    except requests.exceptions.RequestException:
        raise RuntimeError('MESSIF not responding')

    response = json.loads(req.content.decode('utf-8'))
    if response['status']['code'] not in (200, 201):
        print(response)
        raise RuntimeError('MESSIF signalized error')

    messif_ids = [int(record['_id']) for record in response['answer_records']]
    try:
        statistics = {
            'pivotDistCountTotal': response['query_record']['pivotDistCountTotal'],
            'pivotDistCountCached': response['query_record']['pivotDistCountCached'],
            'pivotTime': response['query_record']['pivotDistTimes'],
        }
        if phase in ['sketches_small', 'sketches_large']:
            statistics.update({
                'searchDistCountTotal': 0,
                'searchDistCountCached': 0,
                'searchTime': response['statistics']['OperationTime'],
            })
        else:
            statistics.update({
                'searchDistCountTotal': response['query_record']['searchDistCountTotal'],
                'searchDistCountCached': response['query_record']['searchDistCountCached'],
                'searchTime': response['statistics']['OperationTime'] - response['query_record']['pivotDistTimes'],
            })
    except KeyError:
        print(response)
        raise RuntimeError('MESSIF returned an unexpected response')

    if not messif_ids:
        return [], statistics

    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    query_template = ', '.join(['%s'] * len(messif_ids))
    c.execute(f'SELECT gesamtId FROM proteinChain WHERE intId IN ({query_template})', tuple(messif_ids))
    chain_ids = [candidate[0] for candidate in c.fetchall()]
    c.close()
    conn.close()

    return chain_ids, statistics


def get_similarity_results(query: str, other: str, min_qscore: float) -> Tuple[float, float, float, int, List[float]]:
    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
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
        if elapsed > 30:
            insert_query = (f'INSERT IGNORE INTO queriesNearestNeighboursStats '
                            f'(evaluationTime, queryGesamtId, nnGesamtId,'
                            f' qscore, rmsd, alignedResidues, seqIdentity, rotationStats) '
                            f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)')
            T_str = ';'.join(f'{x:.3f}' for x in T)
            c.execute(insert_query, (elapsed, query, other, qscore, rmsd, aligned, seq_identity, T_str))
            conn.commit()
    else:
        qscore, rmsd, seq_identity, aligned, T = query_result[0]
        T = [float(x) for x in T.split(';')]
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


def get_progress(job_id: str, phase: str) -> dict:
    url = f'http://similar-pdb.cerit-sc.cz:{PORTS[phase]}/get_progress'

    try:
        req = requests.get(url, params={'job_id': job_id})
    except requests.exceptions.RequestException:
        raise RuntimeError('MESSIF not responding')

    response = json.loads(req.content.decode('utf-8'))
    progress = {'running': False}
    try:
        if bool(response['Running']):
            progress.update({
                'running': True,
                'pivotDistCountExpected': response['pivotDistCountExpected'],
                'pivotDistCountCached': response['pivotDistCountCached'],
                'pivotDistCountComputed': response['pivotDistCountComputed'] - response['pivotDistCountCached']
            })
            if phase == 'full' and response['pivotTime'] != -1:
                progress.update({
                    'pivotTime': response['pivotTime'],
                    'searchDistCountExpected': response['searchDistCountExpected'],
                    'searchDistCountCached': response['searchDistCountCached'],
                    'searchDistCountComputed': response['searchDistCountComputed'] - response['searchDistCountCached']
                })
    except KeyError:
        print('Incorrect response when calling ', url)
        print(response)

    return progress


def end_messif_job(job_id: str, phase: str) -> None:
    url = f'http://similar-pdb.cerit-sc.cz:{PORTS[phase]}/end_job'

    try:
        req = requests.get(url, params={'job_id': job_id})
        print('Ending search on ', req.url)
    except requests.exceptions.RequestException:
        raise RuntimeError('MESSIF not responding')
