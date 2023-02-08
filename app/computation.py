import os
import numpy as np
import tempfile
from pathlib import Path
import shutil
import requests
import json
import mariadb
import time
import subprocess

from flask import Request
from typing import List, Tuple, Dict

import python_distance
from .config import config


class DBConnection:
    def __enter__(self):
        self.conn = mariadb.connect(host=config['db']['host'], user=config['db']['user'], password=config['db']['password'],
                                    database=config['db']['database'])
        self.c = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.c.close()
        self.conn.close()


def get_random_pdb_ids(number: int) -> List[str]:
    with DBConnection() as db:
        db.c.execute(f'SELECT gesamtId FROM proteinChain WHERE indexedAsDataObject = 1 ORDER BY RAND() LIMIT %s', (number,))
        pdb_ids = sorted(row[0].split(':')[0] for row in db.c.fetchall())
    return pdb_ids


def get_names(pdb_ids: List[str]) -> Dict[str, str]:
    names = {}
    with DBConnection() as db:
        for pdb_id in pdb_ids:
            db.c.execute('SELECT name FROM protein WHERE pdbId = %s', (pdb_id,))
            data = db.c.fetchall()
            if data:
                names[pdb_id] = data[0][0]
    return names


def search_title(query: str, limit: int) -> List[str]:
    words = ' '.join(f'+{word}*' for word in query.split())
    sql_query = ('SELECT id FROM proteinId WHERE id IN '
                 '(SELECT pdbId FROM protein WHERE MATCH(name) AGAINST (%s IN BOOLEAN MODE)) '
                 'LIMIT %s')
    with DBConnection() as db:
        db.c.execute(sql_query, (words, limit))
        pdb_ids = sorted(row[0].split(':')[0] for row in db.c.fetchall())

    return pdb_ids


def prepare_indexed_chain(pdb_id: str) -> Tuple[str, List[Tuple[str, int]]]:
    with DBConnection() as db:
        db.c.execute('SELECT gesamtId, chainLength FROM proteinChain WHERE gesamtId LIKE %s AND indexedAsDataObject = 1', (f'{pdb_id}%',))
        chains = [(chain_data[0].split(':')[1], chain_data[1]) for chain_data in db.c.fetchall()]

    if not chains:
        raise RuntimeError('No chains having at least 10 residues detected.')

    tmpdir = tempfile.mkdtemp(prefix='query', dir=config['dirs']['computations'])
    os.chmod(tmpdir, 0o755)

    prefix = pdb_id[:2].lower()
    shutil.copy(Path(config['dirs']['raw_pdbs'], f'{pdb_id.lower()}.cif'), Path(tmpdir, 'query'))
    for chain in (chain[0] for chain in chains):
        filename = Path(config['dirs']['archive'], prefix, f'{pdb_id}:{chain}.bin')
        shutil.copy(filename, Path(tmpdir, f'query:{chain}.bin'))

    job_id = Path(tmpdir).name[len('query'):]
    return job_id, chains


def process_input(req: Request) -> Tuple[str, List[Tuple[str, int]]]:
    tmpdir = tempfile.mkdtemp(prefix='query', dir=config['dirs']['computations'])
    os.chmod(tmpdir, 0o755)
    path = Path(tmpdir, 'query')
    req.files['file'].save(path)

    job_id = Path(tmpdir).name[len('query'):]
    chains = python_distance.save_chains(str(Path(tmpdir, 'query')), tmpdir, 'query')
    if not chains:
        raise RuntimeError('No chains having at least 10 residues detected.')

    return job_id, chains


def get_results_messif(query: str, radius: float, num_results: int, phase: str, job_id: str) \
        -> Tuple[List[str], Dict[str, int]]:
    parameters = {'queryid': query, 'k': num_results, 'job_id': job_id}

    if phase in ('sketches_large', 'full'):
        parameters['radius'] = radius

    url = f'http://similar-pdb.cerit-sc.cz:{config["ports"][phase]}/search'
    try:
        req = requests.get(url, params=parameters)
    except requests.exceptions.RequestException as e:
        print(f'ERROR: MESSIF not responding when calling {url}')
        print(f'Original exception: {e}')
        raise RuntimeError('MESSIF not responding')

    try:
        response = json.loads(req.content.decode('utf-8'))
    except json.decoder.JSONDecodeError as e:
        print(f'ERROR: MESSIF not responding when calling {req.url}')
        print(f'Original exception: {e}')
        print(f'Original response: {req.content.decode("utf-8")}')
        raise RuntimeError('MESSIF returned an incorrect response')

    if response['status']['code'] not in (200, 201):
        print(f'ERROR: MESSIF signalized error when calling {req.url}')
        print(f'Response returned: {response}')
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
    except KeyError as e:
        print(f'ERROR: MESSIF returned an incorrect response when calling {req.url}')
        print(f'Key not found: {e}')
        print(f'Original response: {response}')
        raise RuntimeError('MESSIF returned an unexpected response')

    if not messif_ids:
        return [], statistics

    with DBConnection() as db:
        query_template = ', '.join(['%s'] * len(messif_ids))
        db.c.execute(f'SELECT gesamtId FROM proteinChain WHERE intId IN ({query_template})', tuple(messif_ids))
        chain_ids = [candidate[0] for candidate in db.c.fetchall()]

    return chain_ids, statistics


def get_similarity_results(query: str, other: str, min_qscore: float) -> Tuple[float, float, float, int, List[float]]:
    with DBConnection() as db:
        if query == other:
            db.c.execute('SELECT chainLength FROM proteinChain WHERE gesamtId = %s', (query,))
            query_result = db.c.fetchall()
            aligned = -1
            if not query_result:
                print(f'ERROR: Query {query} not found in DB')
            else:
                aligned = query_result[0][0]
            T = np.eye(4).flatten().tolist()
            return 1.0, 0.0, 1.0, aligned, T

        select_query = ('SELECT qscore, rmsd, seqIdentity, alignedResidues, rotationStats '
                        'FROM queriesNearestNeighboursStats WHERE queryGesamtId = %s AND nnGesamtId = %s')
        db.c.execute(select_query, (query, other))
        query_result = db.c.fetchall()
        if not query_result:
            begin = time.time()
            _, qscore, rmsd, seq_identity, aligned, T = python_distance.get_results(query, other, config['dirs']['archive'],
                                                                                    min_qscore)
            end = time.time()
            elapsed = int((end - begin) * 1000)
            results = (qscore, rmsd, seq_identity, aligned, T)
            if elapsed > 30:
                insert_query = ('INSERT IGNORE INTO queriesNearestNeighboursStats '
                                '(evaluationTime, queryGesamtId, nnGesamtId,'
                                ' qscore, rmsd, alignedResidues, seqIdentity, rotationStats) '
                                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)')
                T_str = ';'.join(f'{x:.3f}' for x in T)
                db.c.execute(insert_query, (elapsed, query, other, qscore, rmsd, aligned, seq_identity, T_str))
                db.conn.commit()
        else:
            qscore, rmsd, seq_identity, aligned, T = query_result[0]
            T = [float(x) for x in T.split(';')]
            results = float(qscore), float(rmsd), float(seq_identity), int(aligned), T
    return results


def get_stats(query: str, query_name: str, other: str, min_qscore: float, job_id: str, disable_visualizations: bool) \
        -> Tuple[float, float, float, int]:
    qscore, rmsd, seq_identity, aligned, T = get_similarity_results(query, other, min_qscore)
    directory = Path(config['dirs']['computations'], f'query{job_id}')
    if qscore > min_qscore:
        try:
            query_pdb = Path(directory, 'query.pdb')
            if query == other:
                other_pdb = Path(directory, 'query.pdb')
            else:
                python_distance.prepare_PDB(other, config['dirs']['raw_pdbs'], str(directory), T)
                other_pdb = Path(directory, f'{other}.aligned.pdb')
            if not disable_visualizations:
                output_png = Path(directory, f'{other}.aligned.png')
                args = ['pymol', '-qrc', Path(Path(__file__).parent, 'draw.pml'), '--', query_pdb,
                        other_pdb, output_png]
                subprocess.run(args)

                args = ['convert', '-fill', 'rgb(33, 155, 119)', '-font', 'Carlito-Bold', '-pointsize', '24',
                        '-draw', f'text 20, 40 "{query_name} (query)"', '-fill', 'rgb(192, 85, 25)', '-draw',
                        f'text 20, 70 "{other}"', output_png, output_png]
                subprocess.run(args)
        except:
            print('Cannot generate alignment and image')
    return qscore, rmsd, seq_identity, aligned


def get_progress(job_id: str, phase: str) -> dict:
    url = f'http://similar-pdb.cerit-sc.cz:{config["ports"][phase]}/get_progress'

    try:
        req = requests.get(url, params={'job_id': job_id})
    except requests.exceptions.RequestException as e:
        print(f'ERROR: MESSIF not responding when calling {url} with job_id={job_id}')
        print(f'Original exception: {e}')
        raise RuntimeError('MESSIF not responding')

    try:
        response = json.loads(req.content.decode('utf-8'))
    except json.decoder.JSONDecodeError as e:
        print(f'ERROR: MESSIF not responding when calling {req.url}')
        print(f'Original exception: {e}')
        print(f'Original response: {req.content.decode("utf-8")}')
        raise RuntimeError('MESSIF returned an incorrect response')

    progress = {'running': False}
    try:
        if bool(response['Running']):
            progress.update({
                'running': True,
                'pivotDistCountExpected': response['pivotDistCountExpected'],
                'pivotDistCountCached': response['pivotDistCountCached'],
                'pivotDistCountComputed': max(0, response['pivotDistCountComputed'] - response['pivotDistCountCached'])
            })
            if phase == 'full' and response['pivotTime'] is not None:
                progress.update({
                    'pivotTime': response['pivotTime'],
                    'searchDistCountExpected': response['searchDistCountExpected'],
                    'searchDistCountCached': response['searchDistCountCached'],
                    'searchDistCountComputed': max(0, response['searchDistCountComputed'] - response[
                        'searchDistCountCached'])
                })
    except KeyError as e:
        print(f'ERROR: MESSIF returned an incorrect response when calling {req.url}')
        print(f'Key not found: {e}')
        print(f'Original response: {response}')
        raise RuntimeError('MESSIF returned an unexpected response')

    return progress


def end_messif_job(job_id: str, phase: str) -> None:
    url = f'http://similar-pdb.cerit-sc.cz:{config["ports"][phase]}/end_job'

    try:
        req = requests.get(url, params={'job_id': job_id})
        print('Ending search on ', req.url)
    except requests.exceptions.RequestException as e:
        print(f'ERROR: MESSIF not responding when calling {url}')
        print(f'Original exception: {e}')
        raise RuntimeError('MESSIF not responding')


def prepare_PDB_wrapper(query: str, pdb_dir: str, output_dir: str) -> None:
    python_distance.prepare_PDB(query, pdb_dir, output_dir, None)
