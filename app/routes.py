from flask import render_template, request, flash, send_from_directory, jsonify, redirect, url_for, Response, abort
import concurrent.futures
from datetime import datetime
from typing import Generator, Union
import copy
import re
import sys

from . import application
from .computation import *


@application.route('/', methods=['GET', 'POST'])
def index():
    if not application.db_stats:
        with DBConnection() as db:
            db.c.execute('SELECT COUNT(*) FROM proteinId')
            protein_count = db.c.fetchall()[0][0]
            protein_count = f'{protein_count:,}'.replace(',', ' ')

            db.c.execute('SELECT COUNT(*) FROM proteinChain WHERE indexedAsDataObject = 1')
            chain_count = db.c.fetchall()[0][0]
            chain_count = f'{chain_count:,}'.replace(',', ' ')

            db.c.execute('SELECT DATE(MAX(lastUpdate)) FROM proteinChainMetadata')
            last_update = db.c.fetchall()[0][0]

        application.db_stats = {'protein_count': protein_count, 'chain_count': chain_count, 'updated': last_update}

    if request.method == 'GET':
        return render_template('index.html', **application.db_stats)

    if 'select_pdb_id' in request.form:
        try:
            pdb_id = request.form['pdbid'].upper()
            if not re.match('^[a-z0-9]{4}$', pdb_id, re.IGNORECASE):
                raise RuntimeError()
            job_id, chains = prepare_indexed_chain(pdb_id)
        except RuntimeError:
            flash('Incorrect PDB ID')
            return render_template('index.html', **application.db_stats)
        name = get_names([pdb_id])[pdb_id]
        return render_template('index.html', chains=chains, selected=True, job_id=job_id, input_name=pdb_id,
                               uploaded=False, name=name, **application.db_stats)
    elif 'selected' in request.form:
        pdb_id = request.form['selected']
        try:
            job_id, chains = prepare_indexed_chain(pdb_id)
        except RuntimeError as e:
            flash(f'Internal error: {e}')
            return render_template('index.html', **application.db_stats)
        except FileNotFoundError as e:
            flash(f'Internal error: Required source file not found. ${e}')
            return render_template('index.html', **application.db_stats)

        name = get_names([pdb_id])[pdb_id]
        return render_template('index.html', chains=chains, selected=True, job_id=job_id, input_name=pdb_id,
                               uploaded=False, name=name, **application.db_stats)
    elif 'upload' in request.form:
        try:
            job_id, chains = process_input(request)
        except RuntimeError as e:
            flash(e)
            return render_template('index.html', **application.db_stats)

        filename = request.files['file'].filename
        return render_template('index.html', chains=chains, selected=True, job_id=job_id, input_name=filename,
                               uploaded=True, **application.db_stats)
    else:
        flash('Unknown error')
        return render_template('index.html', **application.db_stats)


@application.route('/search/<string:job_id>', methods=['POST'])
def search(job_id: str):
    chain: str = request.form['chain']
    name: str = request.form['input_name']
    radius: float = 1 - float(request.form['qscore_range'])
    num_results: int = int(request.form['num_results'])
    print(f"started search for chain {chain} under name {name}")
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
        'disable_search_stats': 'disable_search_stats' in request.form,
        'disable_visualizations': 'disable_visualizations' in request.form
    })

    return redirect(url_for('results', job_id=job_id, chain=chain, name=name))


@application.route('/results/<string:job_id>/<string:name>/<string:chain>')
def results(job_id: str, name: str, chain: str):
    if job_id not in application.computation_results:
        abort(404)

    print(f"redirected to jobs chain {chain}")

    disable_search_stats = application.computation_results[job_id]['disable_search_stats']
    disable_visualizations = application.computation_results[job_id]['disable_visualizations']
    title = get_names([name]).get(name, None)
    return render_template('results.html', query=f'{name}:{chain}', job_id=job_id, title=title,
                           disable_search_stats=disable_search_stats, disable_visualizations=disable_visualizations)


@application.route('/details/<string:job_id>/<string:obj>')
def get_details(job_id: str, obj: str):
    if job_id in application.computation_results:
        job_data = application.computation_results[job_id]
        name = job_data['name']
        chain = job_data['chain']
        statistics = job_data['res_data']['statistics']
    else:
        with DBConnection() as db:
            sql_select = 'SELECT name, chain, statistics FROM savedQueries WHERE job_id = %s'
            db.c.execute(sql_select, (job_id,))
            data = db.c.fetchall()

        if not data:
            abort(404)
        name, chain, statistics = data[0]
        statistics = json.loads(statistics)['statistics']

    obj_stats = next((stat for stat in statistics if stat['object'] == obj), None)

    other_pdb = obj.split(':')[0]
    names = get_names([name, other_pdb])
    query_title = names.get(name, None)
    obj_title = names[other_pdb]

    return render_template('details.html', query=f'{name}:{chain}', query_title=query_title, obj_title=obj_title,
                           obj_stats=obj_stats, job_id=job_id, obj=obj)


@application.route('/get_pdb/<string:job_id>/<string:obj>')
def get_pdb(job_id: str, obj: str):
    if obj == 'query':
        file = 'query.pdb'
    else:
        file = f'{obj}.aligned.pdb'

    with open(Path(config['dirs']['computations'], f'query{job_id}', file)) as f:
        return ''.join(line for line in f if not line.startswith('HETATM'))


@application.route('/get_random_pdbs')
def get_random_pdbs() -> Response:
    return jsonify(get_names(get_random_pdb_ids(10)))


@application.route('/get_searched_pdbs/<string:query>')
def get_searched_pdbs(query: str) -> Union[Response, Tuple]:
    if not re.match('^[a-z0-9 ]*$', query, re.IGNORECASE):
        return jsonify({'error': 'Incorrect query.'}), 404

    return jsonify(get_names(search_title(query, 100)))


@application.route('/get_protein_names', methods=['POST'])
def get_protein_names() -> Response:
    return jsonify(get_names(request.get_json()))


@application.route('/get_image/<string:job_id>/<string:obj>')
def get_image(job_id: str, obj: str):
    return send_from_directory(Path(config['dirs']['computations'], f'query{job_id}'), f'{obj}.aligned.png',
                               max_age=0)


def results_event_stream(job_id: str) -> Generator[str, None, None]:
    def set_niceness(val: int):
        os.nice(val)

    job_data = application.computation_results[job_id]

    query_name = f'{job_data["name"]}:{job_data["chain"]}'

    print(f"main loop started for query {query_name}")

    executor = concurrent.futures.ProcessPoolExecutor(initializer=set_niceness, initargs=(19,))

    start_time = time.time()

    query_raw_pdb = executor.submit(prepare_PDB_wrapper, job_data['query'], config['dirs']['raw_pdbs'],
                                    str(Path(config['dirs']['computations'], f'query{job_id}')))

    messif_future = {'sketches_small': executor.submit(get_results_messif, job_data['query'], -1,
                                                       job_data['num_results'], 'sketches_small', job_id),
                     'sketches_large': None,
                     'full': None}

    result_stats = {}
    sent_data = {}
    timer = 0
    end_time = None
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
                print(f"messiff future result {messif_future['full'].result()}")
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
        disable_visualizations = job_data['disable_visualizations']
        min_qscore = 1 - job_data['radius']
        if query_raw_pdb.done():
            print(f"res_data['chain_ids'] {res_data['chain_ids']}")
            for chain_id in res_data['chain_ids']:
                if chain_id not in result_stats:
                    result_stats[chain_id] = executor.submit(get_stats, query, query_name, chain_id, min_qscore, job_id,
                                                             disable_visualizations)

        statistics = []
        completed = 0
        if query_raw_pdb.done():
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

        if res_data['full_status'] == 'DONE':
            res_data['search_time'] = 0
            for phase in ['sketches_small', 'sketches_large', 'full']:
                stats = res_data[f'{phase}_statistics']
                res_data['search_time'] += stats['pivotTime'] + stats['searchTime']

        if res_data['full_status'] == 'DONE' and completed == len(res_data['chain_ids']):
            if end_time is None:
                end_time = time.time()
            res_data['total_time'] = int((time.time() - start_time) * 1000)
            res_data['status'] = 'FINISHED'

        if '_abort' in job_data:
            print('User aborted the search')
            res_data['status'] = 'ABORTED'

        application.computation_results[job_id]['res_data'] = res_data

        if json.dumps(res_data) != json.dumps(sent_data):
            timer = 0
            to_send = copy.deepcopy(res_data)
            # Don't update results table if not necessary
            if json.dumps(sent_data.get('statistics', [])) == json.dumps(to_send['statistics']):
                del to_send['statistics']

            sent_data = copy.deepcopy(res_data)

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

    if sys.version_info.major == 3 and sys.version_info.minor >= 9:
        executor.shutdown(cancel_futures=True)
    else:
        executor.shutdown()


@application.route('/get_results_stream/<string:job_id>')
def stream(job_id: str) -> Union[Response, Tuple]:
    if job_id not in application.computation_results:
        return jsonify({'error': 'job_id not found'}), 404

    return Response(results_event_stream(job_id), mimetype='text/event-stream')


@application.route('/save_query/<string:job_id>')
def save_query(job_id: str):
    if job_id not in application.computation_results:
        return jsonify({'error': 'job_id not found'}), 404

    job_data = application.computation_results[job_id]
    statistics = job_data['res_data']

    with DBConnection() as db:
        sql_insert = ('INSERT IGNORE INTO savedQueries '
                      '(job_id, name, chain, radius, k, statistics, disable_search_stats, disable_visualizations)'
                      'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)')

        db.c.execute(sql_insert, (job_id, job_data['name'], job_data['chain'], job_data['radius'], job_data['num_results'],
                                  json.dumps(statistics), job_data['disable_search_stats'], job_data['disable_visualizations']))
        db.conn.commit()

    return Response(f'{request.url_root}saved_query/{job_id}')


@application.route('/get_txt_results/<string:job_id>')
def get_txt_results(job_id: str):
    all_data = application.computation_results[job_id]
    res_data = all_data['res_data']['statistics']

    filename = f'results_{all_data["query"].replace(":", "_")}.txt'
    with open(Path(config['dirs']['computations'], f'query{job_id}', filename), 'w') as f:
        f.write('Protein chain search results\n')
        f.write(f'{"=" * 40}\n')
        f.write(f'Query: {all_data["name"]}:{all_data["chain"]}\n')
        f.write(f'Q-score threshold: {1 - all_data["radius"]}\n')
        f.write(f'Maximum number of results: {all_data["num_results"]}\n')
        f.write(f'Number of results: {len(res_data)}\n')
        f.write(f'DB version: {application.db_stats["updated"]}\n')
        f.write(f'Results downloaded: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
        f.write(f'{"=" * 40}\n')
        f.write(','.join(['object', 'qscore', 'rmsd', 'seq_id', 'aligned']) + '\n')
        for obj in res_data:
            f.write(f'{obj["object"]},'
                    f'{obj["qscore"]:5.3f},'
                    f'{obj["rmsd"]:5.3f},'
                    f'{obj["seq_id"]:5.3f},'
                    f'{obj["aligned"]}\n')

    return send_from_directory(Path(config['dirs']['computations'], f'query{job_id}'), filename,
                               max_age=0, as_attachment=True)


@application.route('/saved_query/<string:job_id>')
def saved_query(job_id: str):
    dir_exists = Path(config['dirs']['computations'], f'query{job_id}').exists()

    with DBConnection() as db:
        sql_select = ('SELECT name, chain, statistics, added, disable_search_stats, disable_visualizations '
                      'FROM savedQueries WHERE job_id = %s')
        db.c.execute(sql_select, (job_id,))
        data = db.c.fetchall()

    if not dir_exists or not data:
        return Response('Invalid link.')

    name, chain, statistics, added, disable_search_stats, disable_visualizations = data[0]
    title = get_names([name]).get(name, None)

    return render_template('results.html', saved=True, statistics=statistics, query=f'{name}:{chain}', added=added,
                           title=title, disable_search_stats=disable_search_stats,
                           disable_visualizations=disable_visualizations)


@application.route('/end_job/<string:job_id>', methods=['GET', 'POST'])
def end_job(job_id: str):
    if job_id not in application.computation_results:
        return jsonify({'error': 'job_id not found'}), 404

    application.computation_results[job_id]['_abort'] = True
    return '', 204


@application.route('/find_similar/<string:job_id>/<string:obj>')
def find_similar(job_id: str, obj: str):
    pdbid, chain = obj.split(':')

    if job_id in application.computation_results:
        job_data = application.computation_results[job_id]
        k = job_data['num_results']
        radius = job_data['radius']
        disable_visualizations = job_data['disable_visualizations']
        disable_search_stats = job_data['disable_search_stats']
    else:
        with DBConnection() as db:
            sql_select = ('SELECT k, radius, disable_search_stats, disable_visualizations '
                          'FROM savedQueries WHERE job_id = %s')
            db.c.execute(sql_select, (job_id,))
            data = db.c.fetchall()

        if not data:
            abort(404)
        k, radius, disable_search_stats, disable_visualizations = data[0]

    new_job_id, chains = prepare_indexed_chain(pdbid)

    application.computation_results[new_job_id] = application.mp_manager.dict({
        'query': obj,
        'radius': radius,
        'name': pdbid,
        'chain': chain,
        'num_results': k,
        'disable_search_stats': disable_search_stats,
        'disable_visualizations': disable_visualizations
    })

    return redirect(url_for('results', job_id=new_job_id, chain=chain, name=pdbid))


@application.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404
