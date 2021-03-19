from flask import render_template, request, flash, send_from_directory, jsonify, redirect, url_for
import os
import multiprocessing
import python_distance

from . import application
from .config import *
from .computation import *

pool = multiprocessing.Pool()

computation_results = {}


@application.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html')

    if 'select_pdb_id' in request.form:
        try:
            pdb_id = request.form['pdbid'].upper()
            job_id, ids = prepare_indexed_chain(pdb_id)
        except RuntimeError:
            flash('Incorrect PDB ID')
            return render_template('index.html')
        name = get_names([pdb_id])[pdb_id]
        return render_template('index.html', chains=ids, selected=True, job_id=job_id, input_name=pdb_id,
                               uploaded=False, name=name)
    elif 'selected' in request.form:
        pdb_id = request.form['selected']
        try:
            job_id, ids = prepare_indexed_chain(pdb_id)
        except RuntimeError as e:
            flash(f'Internal error: {e}')
            return render_template('index.html')
        except FileNotFoundError:
            flash('Internal error: Required source file not found.')
            return render_template('index.html')

        name = get_names([pdb_id])[pdb_id]
        return render_template('index.html', chains=ids, selected=True, job_id=job_id, input_name=pdb_id,
                               uploaded=False, name=name)
    elif 'upload' in request.form:
        try:
            job_id, ids = process_input(request)
        except RuntimeError as e:
            flash(e)
            return render_template('index.html')

        filename = request.files['file'].filename
        return render_template('index.html', chains=ids, selected=True, job_id=job_id, input_name=filename,
                               uploaded=True)
    else:
        flash('Unknown error')
        return render_template('index.html')


@application.route('/search', methods=['POST'])
def search():
    job_id: str = request.form['job_id']
    chain: str = request.form['chain']
    name: str = request.form['input_name']
    radius: float = 1 - float(request.form['qscore_range'])
    num_results: int = int(request.form['num_results'])
    if request.form['uploaded'] == 'True':
        query = f'_{job_id}:{chain}'
    else:
        query = f'{name}:{chain}'

    computation_results[job_id] = {
        'sketches_small': None,
        'sketches_large': None,
        'full': None,
        'query': query,
        'radius': radius,
        'num_results': num_results,
        'result_stats': {}
    }
    try:
        computation_results[job_id]['sketches_small'] = pool.apply_async(get_results_messif, args=(
            query, -1, num_results, 'sketches_small', job_id))
    except RuntimeError:
        flash('Calculation failed')
        return render_template('index.html')

    python_distance.prepare_PDB(query, RAW_PDB_DIR, os.path.join(COMPUTATIONS_DIR, f'query{job_id}'), None)
    return redirect(url_for('results', job_id=job_id, chain=chain, name=name))


@application.route('/results')
def results():
    job_id: str = request.args.get('job_id')
    chain: str = request.args.get('chain')
    name: str = request.args.get('name')

    return render_template('results.html', query=chain, job_id=job_id, input_name=name)


@application.route('/details')
def get_details():
    chain: str = request.args.get('chain')
    obj: str = request.args.get('object')

    return render_template('details.html', object=obj, query_chain=chain)


@application.route('/get_pdb')
def get_pdb():
    job_id: str = request.args.get('job_id')
    obj: str = request.args.get('object')

    if obj == '_query':
        file = 'query.pdb'
    else:
        file = f'{obj}.aligned.pdb'
    return send_from_directory(os.path.join(COMPUTATIONS_DIR, f'query{job_id}'), file, cache_timeout=0)


@application.route('/get_random_pdbs')
def get_random_pdbs():
    return jsonify(get_names(get_random_pdb_ids(10))), 200


@application.route('/get_searched_pdbs')
def get_searched_pdbs():
    query = request.args.get('query')
    return jsonify(get_names(search_title(query, 1000))), 200


@application.route('/get_protein_names', methods=['POST'])
def get_protein_names():
    return jsonify(get_names(request.get_json())), 200


@application.route('/get_image')
def get_image():
    job_id: str = request.args.get('job_id')
    obj: str = request.args.get('object')

    return send_from_directory(os.path.join(COMPUTATIONS_DIR, f'query{job_id}'), f'{obj}.aligned.png', cache_timeout=0)


@application.route('/get_results')
def get_results():
    job_id: str = request.args.get('job_id')

    job_data = computation_results[job_id]

    res_data = {'chain_ids': [], 'phase': 'none'}

    sketches_small = job_data['sketches_small']
    if sketches_small.ready():
        res_data['chain_ids'], stats = sketches_small.get()
        res_data['phase'] = 'sketches_small'
        res_data['sketches_small_statistics'] = stats

        sketches_large = job_data['sketches_large']
        if sketches_large is not None and sketches_large.ready():
            res_data['chain_ids'], stats = sketches_large.get()
            res_data['phase'] = 'sketches_large'
            res_data['sketches_large_statistics'] = stats

            full = job_data['full']
            if full is not None and full.ready():
                res_data['chain_ids'], stats = full.get()
                res_data['phase'] = 'full'
                res_data['full_statistics'] = stats

    if res_data['phase'] == 'sketches_small' and job_data['sketches_large'] is None:
        job_data['sketches_large'] = pool.apply_async(get_results_messif, args=(
                                job_data['query'], job_data['radius'], job_data['num_results'], 'sketches_large', job_id))

    if res_data['phase'] == 'sketches_large' and job_data['full'] is None:
        job_data['full'] = pool.apply_async(get_results_messif, args=(
                                job_data['query'], job_data['radius'], job_data['num_results'], 'full', job_id))

    query = job_data['query']
    min_qscore = 1 - job_data['radius']
    for chain_id in res_data['chain_ids']:
        if chain_id not in job_data['result_stats']:
            job_data['result_stats'][chain_id] = pool.apply_async(get_stats, args=(query, chain_id, min_qscore, job_id))

    statistics = []
    completed = 0
    for chain_id in res_data['chain_ids']:
        job = job_data['result_stats'][chain_id]
        if job.ready():
            completed += 1
            qscore, rmsd, seq_id, aligned = job.get()
            if qscore < 1 - job_data['radius']:
                continue
            statistics.append({'object': chain_id,
                               'qscore': round(qscore, 3),
                               'rmsd': round(rmsd, 3),
                               'seq_id': round(seq_id, 3),
                               'aligned': aligned})
        else:
            statistics.append({
                'object': chain_id,
                'qscore': -1,
                'rmsd': None,
                'seq_id': None,
                'aligned': None,
            })

    statistics = sorted(statistics, key=lambda x: x['qscore'], reverse=True)
    res_data['statistics'] = statistics
    res_data['completed'] = completed
    res_data['total'] = len(res_data['chain_ids'])
    if res_data['phase'] == 'full' and completed == len(res_data['chain_ids']):
        res_data['status'] = 'FINISHED'
    else:
        res_data['status'] = 'COMPUTING'

    return jsonify(res_data), 200
