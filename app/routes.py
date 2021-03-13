from flask import render_template, request, flash, send_from_directory, jsonify, redirect
import os
import multiprocessing
import python_distance

from . import application
from .config import *
from .computation import process_input, get_results_messif, prepare_indexed_chain, get_stats, get_random_pdb_id

pool = multiprocessing.Pool()

computation_results = {}


@application.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html')

    if 'select_pdb_id' in request.form:
        try:
            pdb_id = request.form['pdbid'].upper()
            comp_id, ids = prepare_indexed_chain(pdb_id)
        except RuntimeError:
            flash('Incorrect PDB ID')
            return render_template('index.html')
        return render_template('index.html', chains=ids, selected=True, comp_id=comp_id, input_name=pdb_id,
                               uploaded=False)
    elif 'select_random' in request.form:
        pdb_id = get_random_pdb_id()
        try:
            comp_id, ids = prepare_indexed_chain(pdb_id)
        except RuntimeError:
            flash('Internal error')
            return render_template('index.html')
        except FileNotFoundError:
            flash('Internal error: Required source file not found')
            return render_template('index.html')

        return render_template('index.html', chains=ids, selected=True, comp_id=comp_id, input_name=pdb_id,
                               uploaded=False)
    elif 'upload' in request.form:
        try:
            comp_id, ids = process_input(request)
        except RuntimeError:
            flash('Cannot process file')
            return render_template('index.html')

        if not ids:
            flash('No chains detected')
            return render_template('index.html')

        filename = request.files['file'].filename
        return render_template('index.html', chains=ids, selected=True, comp_id=comp_id, input_name=filename,
                               uploaded=True)
    else:
        flash('Unknown error')
        return render_template('index.html')


@application.route('/search', methods=['POST'])
def search():
    comp_id: str = request.form['comp-id']
    chain: str = request.form['chain']
    name: str = request.form['input-name']
    radius: float = 1 - float(request.form['qscore-range'])
    num_results: int = int(request.form['num-results'])
    if request.form['uploaded'] == 'True':
        query = f'_{comp_id}:{chain}'
    else:
        query = f'{name}:{chain}'

    computation_results[comp_id] = {
        'sketches_small': None,
        'sketches_large': None,
        'full': None,
        'query': query,
        'radius': radius,
        'num_results': num_results,
        'result_stats': {}

    }
    try:
        computation_results[comp_id]['sketches_small'] = pool.apply_async(get_results_messif, args=(
            query, -1, num_results, 'sketches_small'))
    except RuntimeError:
        flash('Calculation failed')
        return render_template('index.html')

    return redirect(f'/results?comp_id={comp_id}&chain={chain}&name={name}')


@application.route('/results')
def results():
    comp_id: str = request.args.get('comp-id')
    chain: str = request.args.get('chain')
    name: str = request.args.get('name')

    return render_template('results.html', query=chain, comp_id=comp_id, input_name=name)


@application.route('/details')
def get_details():
    comp_id: str = request.args.get('comp_id')
    chain: str = request.args.get('chain')
    obj: str = request.args.get('object')

    python_distance.prepare_aligned_PDBs(f'_{comp_id}:{chain}', obj, RAW_PDB_DIR,
                                         os.path.join(COMPUTATIONS_DIR, f'query{comp_id}'))

    return render_template('details.html', object=obj, query_chain=chain, comp_id=comp_id)


@application.route('/get_pdb')
def get_pdb():
    comp_id: str = request.args.get('comp_id')
    obj: str = request.args.get('object')

    if obj == '_query':
        file = 'query.pdb'
    else:
        file = f'{obj}.aligned.pdb'
    return send_from_directory(os.path.join(COMPUTATIONS_DIR, f'query{comp_id}'), file, cache_timeout=0)


@application.route('/get_results')
def get_results():
    comp_id: str = request.args.get('comp_id')

    comp_data = computation_results[comp_id]

    res_data = {'chain_ids': [], 'phase': 'none'}

    sketches_small = comp_data['sketches_small']
    if sketches_small.ready():
        res_data['chain_ids'], stats = sketches_small.get()
        res_data['phase'] = 'sketches_small'
        res_data['sketches_small_statistics'] = stats

        sketches_large = comp_data['sketches_large']
        if sketches_large is not None and sketches_large.ready():
            res_data['chain_ids'], stats = sketches_large.get()
            res_data['phase'] = 'sketches_large'
            res_data['sketches_large_statistics'] = stats

            full = comp_data['full']
            if full is not None and full.ready():
                res_data['chain_ids'], stats = full.get()
                res_data['phase'] = 'full'
                res_data['full_statistics'] = stats

    if res_data['phase'] == 'sketches_small' and comp_data['sketches_large'] is None:
        comp_data['sketches_large'] = pool.apply_async(get_results_messif, args=(
                                comp_data['query'], comp_data['radius'], comp_data['num_results'], 'sketches_large'))

    if res_data['phase'] == 'sketches_large' and comp_data['full'] is None:
        comp_data['full'] = pool.apply_async(get_results_messif, args=(
                                comp_data['query'], comp_data['radius'], comp_data['num_results'], 'full'))

    query = comp_data['query']
    for chain_id in res_data['chain_ids']:
        if chain_id not in comp_data['result_stats']:
            comp_data['result_stats'][chain_id] = pool.apply_async(get_stats, args=(query, chain_id))

    statistics = []
    completed = 0
    for chain_id in res_data['chain_ids']:
        job = comp_data['result_stats'][chain_id]
        if job.ready():
            completed += 1
            qscore, rmsd, seq_id, aligned = job.get()
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
