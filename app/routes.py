from flask import render_template, request, flash, redirect, url_for
from werkzeug.utils import secure_filename
import random
import os
import subprocess

from . import application
from .config import CHAINS_DIR, ARCHIVE_DIR, PRELOAD_LIST, OBJECTS_COUNT, UPLOAD_DIR


def pick_objects(number):
    objects = []
    for directory in random.choices(os.listdir(ARCHIVE_DIR), k=number):
        filename = random.choice(os.listdir(os.path.join(ARCHIVE_DIR, directory)))
        object_id, _ = os.path.splitext(filename)
        objects.append(object_id)

    return objects


def prepare_query(filename):
    args = ['get_chains', filename, CHAINS_DIR]
    p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode:
        print('Unable to prepare query: ' + p.stderr.decode('utf-8'))
        raise RuntimeError()

    return p.stdout.decode('utf-8').splitlines()


def calculate_distances(query, objects):
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
    filename = secure_filename(file.filename)

    path = os.path.join(UPLOAD_DIR, filename)
    file.save(path)

    try:
        ids = prepare_query(path)
    except RuntimeError:
        flash('Cannot read input file')
        return redirect(url_for('index'))

    if not ids:
        flash('No chains detected')
        return redirect(url_for('index'))

    return render_template('index.html', chains=ids, uploaded=True)


@application.route('/run', methods=['POST'])
def run():
    chain = request.form['chain']

    objects = pick_objects(OBJECTS_COUNT)

    try:
        distances = calculate_distances(chain, objects)
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

    return render_template('results.html', query=chain, dissimilar=dissimilar, distances=OBJECTS_COUNT,
                           timeout=timeout, results=results)
