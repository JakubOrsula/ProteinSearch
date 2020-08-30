from flask import render_template, request, flash, send_from_directory, jsonify, Request
import random
import os
import subprocess
import tempfile

from typing import List, Dict, Tuple

import python_distance

from . import application
from .config import ARCHIVE_DIR, PRELOAD_LIST, OBJECTS_COUNT, COMPUTATIONS_DIR, RAW_PDB_DIR


def pick_objects(number: int) -> List[str]:
    objects = []
    for directory in random.choices(os.listdir(ARCHIVE_DIR), k=number):
        filename = random.choice(os.listdir(os.path.join(ARCHIVE_DIR, directory)))
        object_id, _ = os.path.splitext(filename)
        objects.append(object_id)

    return objects


def calculate_distances(query: str, objects: List[str]) -> List[float]:
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = '/usr/local/lib'
    args = ['java', '-cp', '/usr/local/lib/java_distance.jar:/usr/local/lib/proteins-1.0.jar', 'TestJava', ARCHIVE_DIR,
            PRELOAD_LIST, f'_{query}', *objects]

    p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if p.returncode:
        print('Calculation failed: ' + p.stderr.decode('utf-8'))
        raise RuntimeError()

    distances = []
    for line in p.stdout.decode('utf-8').splitlines():
        distances.append(float(line))

    return distances


def get_results(comp_id: str, chain: str) -> List[Dict[str, float]]:
    objects = pick_objects(OBJECTS_COUNT)
    distances = calculate_distances(f'{comp_id}:{chain}', objects)
    res = []
    for obj, dist in zip(objects, distances):
        if dist < 1:
            res.append({'object': obj, 'qscore': 1 - dist})

    res.sort(key=lambda x: x['qscore'], reverse=True)

    return res


def process_input(req: Request) -> Tuple[str, List[str]]:
    tmpdir = tempfile.mkdtemp(prefix='query', dir=COMPUTATIONS_DIR)
    path = os.path.join(tmpdir, 'query')
    req.files['file'].save(path)

    return os.path.basename(tmpdir), python_distance.save_chains(os.path.join(tmpdir, 'query'), tmpdir),


@application.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html')

    try:
        comp_id, ids = process_input(request)
    except RuntimeError:
        flash('Cannot process file')
        return render_template('index.html')

    if not ids:
        flash('No chains detected')
        return render_template('index.html')

    filename = request.files['file'].filename
    return render_template('index.html', chains=ids, uploaded=True, comp_id=comp_id, filename=filename)


@application.route('/upload', methods=['POST'])
def upload():
    try:
        comp_id, ids = process_input(request)
    except RuntimeError:
        return jsonify({'status': 'failed', 'message': 'Cannot process file'}), 400

    if not ids:
        return jsonify({'status': 'failed', 'message': 'No chains detected'}), 400

    return jsonify({'status': 'ok', 'comp_id': comp_id, 'chains': ids}), 201


@application.route('/run', methods=['POST'])
def run():
    comp_id: str = request.form['comp-id']
    chain: str = request.form['chain']

    try:
        res = get_results(comp_id, chain)
    except RuntimeError:
        return jsonify({'status': 'failed', 'message': 'Calculation failed'}), 400

    return jsonify({'status': 'ok', 'results': res}), 200


@application.route('/results', methods=['POST'])
def results():
    comp_id: str = request.form['comp-id']
    chain: str = request.form['chain']
    filename: str = request.form['filename']

    try:
        res = get_results(comp_id, chain)
    except RuntimeError:
        flash('Calculation failed')
        return render_template('index.html')

    return render_template('results.html', query=chain, no_distances=OBJECTS_COUNT, results=res, comp_id=comp_id,
                           filename=filename)


@application.route('/details')
def get_details():
    comp_id: str = request.args.get('comp_id')
    chain: str = request.args.get('chain')
    obj: str = request.args.get('object')

    result = python_distance.computation_details(f'_{comp_id}:{chain}', obj, RAW_PDB_DIR,
                                                 os.path.join(COMPUTATIONS_DIR, comp_id))

    _, qscore, rmsd, seq_id, aligned = result

    return render_template('details.html', object=obj, query_chain=chain, qscore=qscore, rmsd=rmsd, seq_identity=seq_id,
                           res_aligned=aligned, comp_id=comp_id)


@application.route('/get_pdb')
def get_pdb():
    comp_id: str = request.args.get('comp_id')
    obj: str = request.args.get('object')

    if obj == '_query':
        file = 'query.pdb'
    else:
        file = f'{obj}.aligned.pdb'
    return send_from_directory(os.path.join(COMPUTATIONS_DIR, comp_id), file, cache_timeout=0)
