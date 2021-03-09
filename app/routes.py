from flask import render_template, request, flash, send_from_directory, jsonify
import os
import multiprocessing
import python_distance

from . import application
from .config import *
from .computation import process_input, start_computation, prepare_indexed_chain, get_stats

pool = multiprocessing.Pool()

computation_results = {}


@application.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html')

    input_type = request.form['input-type']

    if input_type == 'file':
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
        try:
            comp_id, ids = prepare_indexed_chain(request)
        except RuntimeError:
            flash('Incorrect PDB ID')
            return render_template('index.html')
        return render_template('index.html', chains=ids, selected=True, comp_id=comp_id,
                               input_name=request.form['pdbid'].upper(), uploaded=False)


@application.route('/results', methods=['POST'])
def results():
    comp_id: str = request.form['comp-id']
    chain: str = request.form['chain']
    name: str = request.form['input-name']
    radius: float = 1 - float(request.form['qscore-range'])
    num_results: int = int(request.form['num-results'])
    if request.form['uploaded'] == 'True':
        query = f'_{comp_id}:{chain}'
        input_type = 'uploaded'
    else:
        query = f'{name}:{chain}'
        input_type = 'database'

    try:
        computation_results[comp_id] = start_computation(query, radius, num_results, pool)
        computation_results[comp_id]['query'] = query
        computation_results[comp_id]['result_stats'] = {}
    except RuntimeError:
        flash('Calculation failed')
        return render_template('index.html')

    return render_template('results.html', query=chain, comp_id=comp_id, input_name=name, input_type=input_type)


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

    sketches_small = comp_data['sketches_small']
    sketches_large = comp_data['sketches_large']
    full = comp_data['full']

    res_data = {'chain_ids': [], 'phase': 'none'}

    if sketches_small.ready():
        res_data['chain_ids'], stats = sketches_small.get()
        res_data['phase'] = 'sketches_small'
        res_data['sketches_small_statistics'] = stats
    if sketches_large.ready():
        res_data['chain_ids'], stats = sketches_large.get()
        res_data['phase'] = 'sketches_large'
        res_data['sketches_large_statistics'] = stats
    if full.ready():
        res_data['chain_ids'], stats = full.get()
        res_data['phase'] = 'full'
        res_data['full_statistics'] = stats

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

    statistics = sorted(statistics, key=lambda x: x['qscore'], reverse=True)
    res_data['statistics'] = statistics
    res_data['completed'] = completed
    res_data['total'] = len(res_data['chain_ids'])
    if res_data['phase'] == 'full' and completed == len(res_data['chain_ids']):
        res_data['status'] = 'FINISHED'
    else:
        res_data['status'] = 'COMPUTING'

    return jsonify(res_data), 200
