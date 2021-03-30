from flask import render_template, request, flash, send_from_directory, jsonify, redirect, url_for, Response
import os
import concurrent.futures
import python_distance
from typing import Generator
import copy
import sys

from . import application
from .config import *
from .computation import *


@application.route('/', methods=['GET', 'POST'])
def index():
    if not application.db_stats:
        conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
        c = conn.cursor()

        c.execute('SELECT COUNT(*) from proteinId')
        protein_count = c.fetchall()[0][0]
        protein_count = f'{protein_count:,}'.replace(',', ' ')

        c.execute('SELECT COUNT(*) from proteinChain')
        chain_count = c.fetchall()[0][0]
        chain_count = f'{chain_count:,}'.replace(',', ' ')

        c.execute('SELECT DATE(MAX(lastUpdate)) from proteinChainMetadata')
        last_update = c.fetchall()[0][0]
        c.close()
        conn.close()

        application.db_stats = {'protein_count': protein_count, 'chain_count': chain_count, 'updated': last_update}

    if request.method == 'GET':
        return render_template('index.html', **application.db_stats)

    if 'select_pdb_id' in request.form:
        try:
            pdb_id = request.form['pdbid'].upper()
            job_id, chains = prepare_indexed_chain(pdb_id)
        except RuntimeError:
            flash('Incorrect PDB ID')
            return render_template('index.html', **application.db_stats)
        name = get_names([pdb_id])[pdb_id]
        return redirect(url_for('index', _anchor='select_chain_phase', chains=chains, selected=True, job_id=job_id,
                                input_name=pdb_id, uploaded=False, name=name, **application.db_stats))
    elif 'selected' in request.form:
        pdb_id = request.form['selected']
        try:
            job_id, chains = prepare_indexed_chain(pdb_id)
        except RuntimeError as e:
            flash(f'Internal error: {e}')
            return render_template('index.html', **application.db_stats)
        except FileNotFoundError:
            flash('Internal error: Required source file not found.')
            return render_template('index.html', **application.db_stats)

        name = get_names([pdb_id])[pdb_id]
        return redirect(url_for('index', _anchor='select_chain_phase', chains=chains, selected=True, job_id=job_id,
                                input_name=pdb_id, uploaded=False, name=name, **application.db_stats))
    elif 'upload' in request.form:
        try:
            job_id, chains = process_input(request)
        except RuntimeError as e:
            flash(e)
            return render_template('index.html', **application.db_stats)

        filename = request.files['file'].filename
        return redirect(url_for('index', _anchor='select_chain_phase', chains=chains, selected=True, job_id=job_id,
                                input_name=filename, uploaded=True, **application.db_stats))
    else:
        flash('Unknown error')
        return render_template('index.html', **application.db_stats)


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

    application.computation_results[job_id] = application.mp_manager.dict({
        'query': query,
        'radius': radius,
        'name': name,
        'chain': chain,
        'num_results': num_results,
    })

    return redirect(url_for('results', job_id=job_id, chain=chain, name=name))


@application.route('/results')
def results():
    job_id: str = request.args.get('job_id')
    chain: str = request.args.get('chain')
    name: str = request.args.get('name')

    return render_template('results.html', query=f'{name}:{chain}', job_id=job_id)


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
def get_random_pdbs() -> Response:
    return jsonify(get_names(get_random_pdb_ids(10)))


@application.route('/get_searched_pdbs')
def get_searched_pdbs() -> Response:
    query = request.args.get('query')
    return jsonify(get_names(search_title(query, 1000)))


@application.route('/get_protein_names', methods=['POST'])
def get_protein_names() -> Response:
    return jsonify(get_names(request.get_json()))


@application.route('/get_image')
def get_image():
    job_id: str = request.args.get('job_id')
    obj: str = request.args.get('object')

    return send_from_directory(os.path.join(COMPUTATIONS_DIR, f'query{job_id}'), f'{obj}.aligned.png', cache_timeout=0)


def results_event_stream(job_id: str) -> Generator[str, None, None]:
    def set_niceness(val: int):
        os.nice(val)

    job_data = application.computation_results[job_id]

    executor = concurrent.futures.ProcessPoolExecutor(initializer=set_niceness, initargs=(19,))
    messif_future = {'sketches_small': executor.submit(get_results_messif, job_data['query'], -1,
                                                       job_data['num_results'], 'sketches_small', job_id),
                     'sketches_large': None,
                     'full': None}

    python_distance.prepare_PDB(job_data['query'], RAW_PDB_DIR, os.path.join(COMPUTATIONS_DIR, f'query{job_id}'), None)

    result_stats = {}
    sent_data = {}
    timer = 0
    print(f'Stream started for job_id = {job_id}')
    while True:
        res_data = {'chain_ids': [],
                    'status': 'COMPUTING',
                    'sketches_small_status': 'COMPUTING',
                    'sketches_large_status': 'WAITING',
                    'full_status': 'WAITING'}

        if messif_future['sketches_small'].done():
            try:
                res_data['chain_ids'], stats = messif_future['sketches_small'].result()
                res_data['sketches_small_statistics'] = stats

                res_data['sketches_small_status'] = 'DONE'
                res_data['sketches_large_status'] = 'COMPUTING'

                if messif_future['sketches_large'] is None:
                    messif_future['sketches_large'] = executor.submit(get_results_messif, job_data['query'],
                                                                      job_data['radius'], job_data['num_results'],
                                                                      'sketches_large', job_id)
            except RuntimeError as e:
                res_data['status'] = 'ERROR'
                res_data['sketches_small_status'] = 'ERROR'
                res_data['error_message'] = str(e)

        if messif_future['sketches_large'] is not None and messif_future['sketches_large'].done():
            try:
                res_data['chain_ids'], stats = messif_future['sketches_large'].result()
                res_data['sketches_large_statistics'] = stats

                res_data['sketches_large_status'] = 'DONE'
                res_data['full_status'] = 'COMPUTING'

                if messif_future['full'] is None:
                    messif_future['full'] = executor.submit(get_results_messif, job_data['query'], job_data['radius'],
                                                            job_data['num_results'], 'full', job_id)

            except RuntimeError as e:
                res_data['status'] = 'ERROR'
                res_data['sketches_large_status'] = 'ERROR'
                res_data['error_message'] = str(e)

        if messif_future['full'] is not None and messif_future['full'].done():
            try:
                res_data['chain_ids'], stats = messif_future['full'].result()
                res_data['full_status'] = 'DONE'
                res_data['full_statistics'] = stats
            except RuntimeError as e:
                res_data['status'] = 'ERROR'
                res_data['full_status'] = 'ERROR'
                res_data['error_message'] = str(e)

        running_phase = None
        for phase in ['sketches_small', 'sketches_large', 'full']:
            if res_data[f'{phase}_status'] == 'COMPUTING':
                running_phase = phase

        try:
            if running_phase is not None:
                res_data[f'{running_phase}_progress'] = get_progress(job_id, running_phase)
        except RuntimeError as e:
            res_data['status'] = 'ERROR'
            res_data[f'{running_phase}_status'] = 'ERROR'
            res_data['error_message'] = str(e)

        query = job_data['query']
        min_qscore = 1 - job_data['radius']
        for chain_id in res_data['chain_ids']:
            if chain_id not in result_stats:
                result_stats[chain_id] = executor.submit(get_stats, query, chain_id, min_qscore, job_id)

        statistics = []
        completed = 0
        for chain_id in res_data['chain_ids']:
            job = result_stats[chain_id]
            if job.done():
                completed += 1
                qscore, rmsd, seq_id, aligned = job.result()
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

        if res_data['full_status'] == 'DONE' and completed == len(res_data['chain_ids']):
            res_data['status'] = 'FINISHED'

        if '_abort' in job_data:
            print('User aborted the search')
            res_data['status'] = 'ABORTED'

        if json.dumps(res_data) != json.dumps(sent_data):
            timer = 0
            to_send = copy.deepcopy(res_data)
            # Don't update results table if not necessary
            if json.dumps(sent_data.get('statistics', [])) == json.dumps(to_send['statistics']):
                del to_send['statistics']

            sent_data = to_send

            yield 'data: ' + json.dumps(to_send) + '\n\n'
        else:
            if timer == 5:
                timer = 0
                yield ': keep-alive'

        timer += 1
        time.sleep(1)
        if res_data['status'] in ['FINISHED', 'ERROR', 'ABORTED']:
            break

    print(f'Stream ended for job_id = {job_id}')
    for phase in ['sketches_small', 'sketches_large', 'full']:
        if res_data[f'{phase}_status'] == 'COMPUTING':
            end_messif_job(job_id, phase)

    application.computation_results[job_id]['res_data'] = res_data

    if sys.version_info.major == 3 and sys.version_info.minor >= 9:
        executor.shutdown(cancel_futures=True)
    else:
        executor.shutdown()


@application.route('/get_results_stream')
def stream() -> Response:
    job_id: str = request.args.get('job_id')
    return Response(results_event_stream(job_id), mimetype='text/event-stream')


@application.route('/save_query')
def save_query():
    job_id: str = request.args.get('job_id')
    job_data = application.computation_results[job_id]
    statistics = job_data['res_data']

    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()
    sql_insert = ('INSERT IGNORE INTO savedQueries '
                  '(job_id, name, chain, radius, k, statistics) VALUES (%s, %s, %s, %s, %s, %s)')

    c.execute(sql_insert, (job_id, job_data['name'], job_data['chain'], job_data['radius'], job_data['num_results'],
                           json.dumps(statistics)))
    conn.commit()
    c.close()
    conn.close()

    return Response(f'{request.url_root}saved_query?job_id={job_id}')


@application.route('/saved_query')
def saved_query():
    job_id: str = request.args.get('job_id')

    dir_exists = os.path.exists(os.path.join(COMPUTATIONS_DIR, f'query{job_id}'))
    conn = mariadb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    c = conn.cursor()

    sql_select = 'SELECT name, chain, radius, k, statistics, added FROM savedQueries WHERE job_id = %s'
    c.execute(sql_select, (job_id,))
    data = c.fetchall()
    c.close()
    conn.close()

    if not dir_exists or not data:
        return Response('Invalid link.')

    name, chain, radius, k, statistics, added = data[0]

    return render_template('results.html', saved=True, statistics=statistics, query=f'{name}:{chain}', added=added)


@application.route('/end_job', methods=['GET', 'POST'])
def end_job():
    job_id: str = request.args.get('job_id')
    application.computation_results[job_id]['_abort'] = True
    return '', 204
