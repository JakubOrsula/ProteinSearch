import argparse
import json
import os.path
from concurrent.futures import as_completed, ThreadPoolExecutor
import sys
import random
import mariadb
import string
import tqdm
import time
import requests
from jsonschema import validate, ValidationError
from typing import List

sys.path.append('/usr/local/www/ProteinSearch')

from config import *


def get_ids(min_size: int, max_size: int, limit: int) -> List[str]:
    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    sql_select = ('SELECT gesamtId, chainLength FROM proteinChain'
                  ' WHERE chainLength BETWEEN %s AND %s ORDER BY RAND() LIMIT %s')
    c.execute(sql_select, (min_size, max_size, limit))
    data = [row[0] for row in c.fetchall()]
    c.close()
    conn.close()
    return data


def run_search(pdb_id: str, num_results: int, radius: float, schema: str, sleep: bool):
    if sleep:
        time.sleep(random.random())
    url = f'http://similar-pdb.cerit-sc.cz:{PORTS["full"]}/search'
    job_id = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))
    parameters = {'queryid': pdb_id, 'k': num_results, 'job_id': job_id, 'radius': radius}
    try:
        req = requests.get(url, params=parameters)
    except requests.exceptions.RequestException as e:
        return pdb_id, str(e)
    response = json.loads(req.content.decode('utf-8'))
    try:
        validate(response, schema=schema)
    except ValidationError as e:
        return pdb_id, str(e)
    return pdb_id, 'OK'


def stress(ids: List[str], num_results: int, radius: float, workers: int, sleep: bool):
    with open(os.path.join(os.path.dirname(__file__), 'response_schema.json')) as f:
        schema = json.load(f)

    executor = ThreadPoolExecutor(workers)

    jobs = [executor.submit(run_search, data, num_results=num_results, radius=radius, schema=schema, sleep=sleep)
            for data in ids]
    results = []
    for job in tqdm.tqdm(as_completed(jobs), total=len(jobs)):
        results.append((job.result()))

    bad = 0
    for pdb_id, res in results:
        if res != 'OK':
            bad += 1
            print(pdb_id)
            print(res)
            print('----')

    print('Summary: ', end='')
    if bad:
        print(f'{bad} bad out of {len(ids)}')
    else:
        print(f'{len(ids)} OK')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=10, help='Number of requests')
    parser.add_argument('--min-size', type=int, default=100, help='Minimal chain size')
    parser.add_argument('--max-size', type=int, default=1000, help='Maximal chain size')
    parser.add_argument('--radius', type=float, default=0.5, help='Radius for search')
    parser.add_argument('--results', type=int, default=30, help='Number of results for search')
    parser.add_argument('--workers', type=int, default=1, help='Number of concurrent requests')
    parser.add_argument('--sleep', dest='sleep', action='store_true', default=False,
                        help='Random sleep before request')

    args = parser.parse_args()

    pdb_ids = get_ids(args.min_size, args.max_size, args.count)

    stress(pdb_ids, num_results=args.results, radius=args.radius, workers=args.workers, sleep=args.sleep)
