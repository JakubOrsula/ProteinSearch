from flask import render_template, request, flash, redirect, url_for, send_from_directory
import random
import os
import subprocess
import tempfile

from typing import List

import python_distance

from . import application
from .config import ARCHIVE_DIR, PRELOAD_LIST, OBJECTS_COUNT, COMPUTATIONS_DIR


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


@application.route('/')
def index():
    return render_template('index.html', chains=[], uploaded=False)


@application.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']

    tmpdir = tempfile.mkdtemp(prefix='query', dir=COMPUTATIONS_DIR)
    path = os.path.join(tmpdir, 'query')
    file.save(path)

    try:
        ids = python_distance.save_chains(os.path.join(tmpdir, 'query'), tmpdir)
    except RuntimeError:
        flash('Cannot read input file')
        return redirect(url_for('index'))

    if not ids:
        flash('No chains detected')
        return redirect(url_for('index'))

    return render_template('index.html', chains=ids, uploaded=True, comp_id=os.path.basename(tmpdir))


@application.route('/run', methods=['GET', 'POST'])
def run():
    comp_id: str = request.args.get('comp_id')
    chain: str = request.form['chain']

    objects: List[str] = pick_objects(OBJECTS_COUNT)

    try:
        distances = calculate_distances(f'{comp_id}:{chain}', objects)
    except RuntimeError:
        flash('Calculation failed')
        return render_template('index.html', chains=[], uploaded=False)

    results = []
    dissimilar = 0
    timeout = 0
    for obj, dist in zip(objects, distances):
        if dist < 1:
            results.append((obj, 1 - dist))
        elif dist == 2:
            dissimilar += 1
        elif dist == 3:
            timeout += 1

    results.sort(key=lambda x: x[1], reverse=True)

    return render_template('results.html', query=chain, no_distances=OBJECTS_COUNT, results=results, comp_id=comp_id)


@application.route('/details')
def get_details():
    comp_id: str = request.args.get('comp_id')
    chain: str = request.args.get('chain')
    obj: str = request.args.get('object')

    python_distance.init_library(ARCHIVE_DIR, PRELOAD_LIST, True, 0.6, 10)
    result = python_distance.computation_details(f'_{comp_id}:{chain}', obj, -1, os.path.join(COMPUTATIONS_DIR, comp_id))

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
