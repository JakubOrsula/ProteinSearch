'use strict';

const PHASE_NAMES = {
    sketches_small: 'Small sketches',
    sketches_large: 'Large sketches',
    full: 'PPP codes + sketches'
};


function format_time(time_in_ms) {
    if (time_in_ms < 60000) {
        return `${time_in_ms.toLocaleString('cs-CZ')} ms`;
    } else {
        const time_in_s = Math.floor(time_in_ms / 1000);
        const minutes = Math.floor(time_in_s / 60);
        const seconds = time_in_s % 60;

        return `${minutes} m ${seconds} s`;
    }
}


function init_index() {
    let $file = $('#file');
    let $upload = $('#upload');
    let $pdbid = $('#pdbid');
    let $select_pdb_id = $('#select_pdb_id');
    let $select_random = $('#select_random');
    let $pdbs = $('#pdbs');
    let $search_input = $('#search_phrase');
    let $search_pdb = $('#search_pdb');
    let $status = $('#status');

    $search_input.on('keypress', function (event) {
        if (event.keyCode === 13 && $search_input.val().length > 2) {
            $search_pdb.trigger('click');
            event.preventDefault();
        }
    })

    $search_input.on('input', function () {
        const val = $search_input.val();
        if (val.match(/^[a-z0-9 ]*$/i)) {
            $search_input.removeClass('is-invalid');
        } else {
            $search_input.addClass('is-invalid');
            $search_pdb.prop('disabled', true);
            return;
        }
        if (val.length > 2) {
            $search_pdb.prop('disabled', false);
        } else {
            $search_pdb.prop('disabled', true);
        }
    })

    $select_random.on('click', function () {
        $.ajax({
                url: '/get_random_pdbs',
                success: function (data) {
                    $pdbs.empty();
                    for (const [pdb_id, name] of Object.entries(data)) {
                        $pdbs.append(`<button form="protein_form" type="submit"
                                        class="list-group-item list-group-item-action"
                                        name="selected" value="${pdb_id}"><b>${pdb_id}</b> ${name}</button>`);
                    }
                    $status.html('Showing 10 random proposals.');
                    $('#protein_select_window').modal();
                }
            }
        )
    })

    $search_pdb.on('click', function () {
        const query = $search_input.val().trim();
        $search_pdb.html('<span id="running" class="spinner-border spinner-border-sm" role="status"></span>');
        $.ajax({
                url: `/get_searched_pdbs/${query}`,
                success: function (data) {
                    $pdbs.empty();
                    const num_results = Object.keys(data).length;
                    for (const [pdb_id, name] of Object.entries(data)) {
                        $pdbs.append(`<button form="protein_form" type="submit"
                                      class="list-group-item list-group-item-action"
                                      name="selected" value="${pdb_id}"><b>${pdb_id}</b> ${name}</button>`);
                        $status.html(`Showing ${num_results} results.`);
                    }
                    if (num_results === 100) {
                        $status.html('Showing only the first 100 results. Please try to narrow your query.');
                    }
                    if (num_results === 0) {
                        $status.html('No results found.');
                    }
                    $search_pdb.html('Search');
                    $('#protein_select_window').modal();
                }
            }
        )
    })

    $file.on('change', function () {
        if ($file.val()) {
            $upload.prop('disabled', false);
        } else {
            $upload.prop('disabled', true);
        }
    });

    $pdbid.on('input', function () {
        const val = $pdbid.val().trim();
        $pdbid.val(val);
        if (val.match(/^[a-z0-9]{4}$/i)) {
            $select_pdb_id.prop('disabled', false);
        } else {
            $select_pdb_id.prop('disabled', true);
        }
    })
}


function fetch_titles(pdbids) {
    $.ajax({
        url: `/get_protein_names`,
        data: JSON.stringify(pdbids),
        type: 'POST',
        contentType: 'application/json',
        dataType: 'json',
        success: function (data) {
            for (const [pdbid, title] of Object.entries(data)) {
                localStorage[pdbid] = title;
                $(`.name_${pdbid}`).html(title);
            }
        }
    })
}


function init_results() {
    const job_id = window.location.pathname.split('/')[2];

    let statusTable = $('#messif_stats').DataTable({
        ordering: false,
        paging: false,
        searching: false,
        info: false,
        columnDefs: [
            {
                targets: [2, 3, 4, 5, 6],
                className: 'text-right'
            },
            {
                targets: [0],
                width: '180px'
            },
            {
                targets: [1],
                width: '70px',
                className: 'text-center'
            },
            {
                targets: [2, 4],
                width: '300px'
            },
            {
                targets: [3, 5, 6],
                width: '80px'
            }
        ]
    });

    for (const phase of ['sketches_small', 'sketches_large', 'full']) {
        statusTable.row.add([`<b>${PHASE_NAMES[phase]}</b>`, '', '', '', '', '', '']).node().id = `stats_row_${phase}`;
    }
    statusTable.draw();

    let resultsTable = $('#table').DataTable({
        columns: [
            {title: 'No.', width: '80px'},
            {title: 'Chain ID', width: '80px'},
            {title: 'Protein (link to PDBe)'},
            {title: 'Q-score', width: '70px', className: 'text-right'},
            {title: 'RMSD', width: '70px', className: 'text-right'},
            {title: 'Aligned res.', width: '90px', className: 'text-right'},
            {title: 'Seq. identity', width: '90px', className: 'text-right'},
            {
                title: 'Alignment',
                'searchable': false,
                'orderable': false,
                width: '100px',
                className: 'small_padding zoom'
            },
        ],
        searching: false,
        paging: false,
        scrollCollapse: true,
        scrollResize: true,
        scrollY: 100,
        info: false,
        language: {
            emptyTable: 'Searching... No similar protein chains found yet.'
        }
    });

    $(window).bind('beforeunload', function () {
        // Modern browsers ignore this message
        return 'Computation is still running, do you want to leave the page and end the search?'
    });

    $(window).bind('unload', function () {
        navigator.sendBeacon(`/end_job/${job_id}`);
    });

    let $save_query = $('#save_query');
    let $stop_search = $('#stop_search');
    let $back = $('#back');
    let $saved_query_url = $('#saved_query_url');
    let $clipboard_copy = $('#clipboard_copy');
    let $copy_status = $('#copy_status');
    let $save_query_close = $('#save_query_close');

    $back.toggle(false);
    $save_query.toggle(false);

    $save_query.on('click', function () {
        $.ajax({
            url: `/save_query/${job_id}`,
            success: function (data) {
                $saved_query_url.val(data);
            }
        })
    });

    $clipboard_copy.on('click', function () {
        navigator.clipboard.writeText($saved_query_url.val()).then(function () {
                $copy_status.text('Successfully copied to clipboard.')
            }, function (error) {
                $copy_status.text(error);
            }
        )
    });

    $save_query_close.on('click', function () {
        $copy_status.text('');
    })

    $stop_search.on('click', function () {
        $.ajax({
            url: `/end_job/${job_id}`,
        });
    });

    $back.on('click', function () {
        location.href = '/';
    });

    function update_content(data) {
        if (['FINISHED', 'ERROR', 'ABORTED'].includes(data['status'])) {
            eventSource.close();
            $stop_search.toggle(false);
            $back.toggle(true);
            $(window).off('beforeunload unload');
        }

        if (data['status'] === 'FINISHED') {
            $save_query.toggle(true);
            $('#total_time').html(`Total time: ${format_time(data['total_time'])}`);
            $('.dataTables_empty').html('No similar protein chains found in the database.')
            $('#results_number').html(`(${data['chain_ids'].length} results)`);
        } else if (data['status'] === 'ABORTED') {
            let $running = $('#running');
            if ($running.length) {
                $running.removeClass(['spinner-border', 'spinner-border-sm']);
                $running.html('<i class="bi bi-x"></i>');
            }
            eventSource.close();
            return;
        }

        for (const phase of ['sketches_small', 'sketches_large', 'full']) {
            const status = data[`${phase}_status`];
            let row_data = [`<b>${PHASE_NAMES[phase]}</b>`];
            if (status === 'DONE') {
                const stats = data[`${phase}_statistics`];

                let search_part = '-';
                if (phase === 'full') {
                    search_part = `${stats['searchDistCountTotal']} (computed:
                                            ${stats['searchDistCountTotal'] - stats['searchDistCountCached']}, 
                                            cached: ${stats['searchDistCountCached']})`;

                    $('#search_time').html(`Search time: ${format_time(data['search_time'])}`);
                }

                row_data.push('<i class="bi bi-check"></i>',
                    `${stats['pivotDistCountTotal']} (computed: 
                     ${stats['pivotDistCountTotal'] - stats['pivotDistCountCached']}, 
                     cached: ${stats['pivotDistCountCached']})`,
                    format_time(stats['pivotTime']),
                    search_part,
                    format_time(stats['searchTime']),
                    `<b>${format_time(stats['pivotTime'] + stats['searchTime'])}</b>`);
            } else if (status === 'COMPUTING') {
                const progress_key = `${phase}_progress`;
                let pivots_distances = '';
                let search_distances = '';
                let pivot_time = '';
                if (data.hasOwnProperty(progress_key) && data[progress_key]['running']) {
                    const progress = data[progress_key];

                    if (progress.hasOwnProperty('pivotTime')) {
                        const done = progress['searchDistCountComputed'] + progress['searchDistCountCached'];
                        const percentage = Math.floor(100 * done / progress['searchDistCountExpected']);
                        search_distances = `${percentage}% (done ${done} out of ${progress['searchDistCountExpected']})`;
                        pivot_time = format_time(progress['pivotTime']);
                        pivots_distances = `${progress['pivotDistCountExpected']} ` +
                            `(computed: ${progress['pivotDistCountComputed']}, ` +
                            `cached: ${progress['pivotDistCountCached']})`;
                    } else {
                        const done = progress['pivotDistCountComputed'] + progress['pivotDistCountCached'];
                        const percentage = Math.floor(100 * done / progress['pivotDistCountExpected']);
                        pivots_distances = `${percentage}% (done ${done} out of ${progress['pivotDistCountExpected']})`;
                    }
                }

                row_data.push('<div id="running" class="spinner-border spinner-border-sm" role="status" />',
                    pivots_distances, pivot_time, search_distances, '', '');
            } else if (status === 'WAITING') {
                row_data.push('<i class="bi bi-question"></i>', '', '', '', '', '');
            } else {
                // Error occurred
                row_data.push('<i class="bi bi-exclamation-diamond"></i>',
                    `<span class="text-danger">Error: ${data['error_message']}</span>`,
                    '', `<span class="text-danger">Search aborted</span>`, '', '');
            }

            let old_row_data = statusTable.row(`#stats_row_${phase}`).data();
            let new_data = []
            for (let i = 0; i < 7; i++) {
                new_data.push(row_data[i] !== '' ? row_data[i] : old_row_data[i]);
            }

            statusTable.row(`#stats_row_${phase}`).data(new_data);
        }

        statusTable.columns.adjust().draw();

        // No change in statistics -> skip the rest
        if (!data.hasOwnProperty('statistics')) {
            return;
        }

        resultsTable.clear().draw();

        // Get title of proteins
        let no_titles = []
        for (const res of data['statistics']) {
            const pdbid = res['object'].split(':')[0];
            if (localStorage.getItem(pdbid) === null) {
                no_titles.push(pdbid);
            }
        }
        if (no_titles.length) {
            fetch_titles(no_titles);
        }

        let idx = 0;
        for (const res of data['statistics']) {
            const pdbid = res['object'].split(':')[0];
            let name = localStorage.getItem(pdbid) === null ? '?' : localStorage.getItem(pdbid);

            let qscore = '?';
            let rmsd = '?';
            let aligned = '?';
            let seq_id = '?';
            let link = `<img src="/static/empty.png" alt="Alignment thumbnail of ${res['object']}">`;

            if (res['qscore'] !== -1) {
                qscore = res['qscore'].toFixed(3);
                rmsd = res['rmsd'].toFixed(3);
                aligned = res['aligned'];
                seq_id = res['seq_id'].toFixed(3);
                link = `<a href="/details/${job_id}/${res['object']}" target="_blank">
                                <div class="zoom-text">Show 3D visualization</div>
                                <img src="/get_image/${job_id}/${res['object']}"
                                     alt="Alignment thumbnail of ${res['object']}">
                                </a>`;
            }

            const data = [idx + 1, `<div>${res['object']}</div><div class="mt-1" style="max-width: 75px"><a href="/find_similar/${job_id}/${res['object']}">Find similar to this</a></div>`,
                `<a href="https://www.ebi.ac.uk/pdbe/entry/pdb/${pdbid}" target="_blank" rel="noreferrer">
                            <div class="name_${pdbid}" style="max-width: 900px">${name}</div>
                        </a>`, qscore, rmsd, aligned, seq_id, link];

            resultsTable.row.add(data).node().id = res['object'];
            idx++;
        }
        resultsTable.columns.adjust().draw();
    }

    const eventSource = new EventSource(`/get_results_stream/${job_id}`);
    if (window.location.pathname.startsWith('/results')) {
        eventSource.onmessage = function (e) {
            const data = JSON.parse(e.data);
            update_content(data);
        }
    } else {
        const data = JSON.parse(saved_statistics);
        update_content(data);
    }

}


function load_molecule(plugin, job_id, name, index) {

    const id = index === 0 ? `${name} (query)`: name;
    const object = index === 0 ? 'query' : name;

    plugin.loadMolecule({
        id: id,
        format: 'pdb',
        url: `/get_pdb/${job_id}/${object}`,
        modelRef: 'object-model' + index,
        doNotCreateVisual: true
    }).then(
        () => {
            let colors = LiteMol.Bootstrap.Immutable.Map();
            const color = index === 0 ? {r: 0.129, g: 0.607, b: 0.466} : {r: 0.752, g: 0.333, b: 0.098};
            let style = {
                type: 'Cartoons',
                params: {detail: 'Automatic', showDirectionCone: false},
                theme: {
                    template: LiteMol.Bootstrap.Visualization.Molecule.Default.UniformThemeTemplate,
                    colors: colors.set('Uniform', color),
                    transparency: {}
                }
            };

            const t = plugin.createTransform();
            t.add('object-model' + index, LiteMol.Bootstrap.Entity.Transformer.Molecule.CreateVisual, {style: style})
            plugin.applyTransform(t);
        }
    )
}


function init_details() {
    let plugin = LiteMol.Plugin.create({
        target: '#litemol',
        viewportBackground: '#fff',
        layoutState: {
            hideControls: true,
        },
    });

    const query_name = $('#query').text();
    const other_name = $('#other').text();

    const parts = window.location.pathname.split('/')
    const job_id = parts[2];

    load_molecule(plugin, job_id, query_name, 0);
    load_molecule(plugin, job_id, other_name, 1);
}


$(function () {
    const page = window.location.pathname;
    if (page === '/') {
        init_index();
    } else if (page.startsWith('/results') || page.startsWith('/saved_query')) {
        init_results();
    } else if (page.startsWith('/details')) {
        init_details();
    }
})
;
