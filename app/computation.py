import os
import subprocess
import tempfile
import multiprocessing.pool

from flask import Request
from typing import List, Tuple, Dict

import python_distance
from .config import ARCHIVE_DIR, COMPUTATIONS_DIR


def process_input(req: Request) -> Tuple[str, List[str]]:
    tmpdir = tempfile.mkdtemp(prefix='query', dir=COMPUTATIONS_DIR)
    path = os.path.join(tmpdir, 'query')
    req.files['file'].save(path)

    return os.path.basename(tmpdir), python_distance.save_chains(os.path.join(tmpdir, 'query'), tmpdir)


def get_candidates(query: str) -> List[str]:
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = '/usr/local/lib'
    args = ['java', '-cp', '/usr/local/lib/get_candidates.jar', 'GetCandidates', ARCHIVE_DIR]

    p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if p.returncode:
        print('Calculation failed: ' + p.stderr.decode('utf-8'))
        raise RuntimeError()

    distances = []
    for line in p.stdout.decode('utf-8').splitlines():
        distances.append(line.strip())

    return distances


def compute_distance(comp_id: str, chain: str, candidate: str) -> None:
    python_distance.init_library(ARCHIVE_DIR, '/dev/null', True, 0, 10)
    res = python_distance.get_results(f'_{comp_id}:{chain}', candidate, ARCHIVE_DIR)
    return res


def start_computation(comp_id: str, chain: str, pool: multiprocessing.Pool) ->\
        Dict[str, multiprocessing.pool.AsyncResult]:

    candidates = get_candidates(f'_{comp_id}:{chain}')
    results = {}
    for candidate in candidates:
        results[candidate] = pool.apply_async(compute_distance, args=(comp_id, chain, candidate))

    return results
