from flask import render_template, request
from werkzeug.utils import secure_filename
import random
import os
import subprocess

from . import application
from .config import QUERIES_DIR, ARCHIVE_DIR, PRELOAD_LIST, OBJECTS_COUNT


def pick_objects(number):
    objects = []
    for directory in random.choices(os.listdir(ARCHIVE_DIR), k=number):
        filename = random.choice(os.listdir(os.path.join(ARCHIVE_DIR, directory)))
        object_id, _ = os.path.splitext(filename)
        objects.append(object_id)

    return objects


def calculate_distances(query, objects):
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = '/usr/local/lib'
    args = ['java', '-cp', '/usr/local/lib/java_distance.jar:/usr/local/lib/proteins-1.0.jar', 'TestJava', ARCHIVE_DIR,
            PRELOAD_LIST, query, *objects]

    p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if p.returncode:
        raise RuntimeError('Calculation failed')

    distances = []
    for line in p.stdout.decode('utf-8').splitlines():
        distances.append(float(line))

    return distances


@application.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        chain = request.form['chain'].upper()
        file = request.files['file']
        filename = secure_filename(file.filename).upper()

        basename, ext = os.path.splitext(filename)

        ext = ext.lower()
        if ext == '.pdb':
            prefix = 'p'
        else:
            prefix = 'c'

        i = 0
        while os.path.exists(os.path.join(QUERIES_DIR, f'{prefix}{i:02d}{ext}')):
            i += 1

        new_id = f'{prefix}{i:02d}'

        path = os.path.join(QUERIES_DIR, f'{new_id}{ext}')
        file.save(path)

        query_id = f'_{new_id}:{chain}'

        objects = pick_objects(OBJECTS_COUNT)

        distances = calculate_distances(query_id, objects)
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

        query = f'{basename}:{chain}'

        return render_template('results.html', query=query, dissimilar=dissimilar, distances=OBJECTS_COUNT,
                               timeout=timeout, results=results)

    return render_template('index.html')
