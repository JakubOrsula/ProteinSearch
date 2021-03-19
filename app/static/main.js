'use strict';

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
        }
    })

    $search_input.on('input', function () {
        if ($search_input.val().length > 2) {
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
                        $pdbs.append(`<button type="submit" class="list-group-item list-group-item-action"
                                                name="selected" value="${pdb_id}"><b>${pdb_id}</b> ${name}</button>`);
                    }

                    $status.html('Displaying 10 random results');
                }
            }
        )
    })

    $search_pdb.on('click', function () {
        const query = $search_input.val();
        $.ajax({
                url: `/get_searched_pdbs?query=${query}`,
                success: function (data) {
                    $pdbs.empty();
                    const num_results = Object.keys(data).length;
                    for (const [pdb_id, name] of Object.entries(data)) {
                        $pdbs.append(`<button type="submit" class="list-group-item list-group-item-action"
                                                name="selected" value="${pdb_id}"><b>${pdb_id}</b> ${name}</button>`);
                        $status.html(`Displaying ${num_results} result(s)`);
                    }
                    if (num_results === 1000) {
                        $status.html('Displaying first 1000 results');
                    }
                    if (num_results === 0) {
                        $status.html('No results found');
                    }
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
        let val = $pdbid.val();
        if (val.length === 4) {
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
                $(`.name_${pdbid}`).val(title);
            }
        }
    })
}


function init_results() {
    const parameters_string = window.location.search;
    const parameters = new URLSearchParams(parameters_string);

    let object_params = new URLSearchParams();
    const comp_id = parameters.get('comp_id');
    object_params.set('comp_id', comp_id);

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
                targets: [1],
                className: 'text-center'
            }
        ]
    });

    let resultsTable = $('#table').DataTable({
        columns: [
            {title: 'No.', width: '80px'},
            {title: 'Chain ID', width: '80px'},
            {title: 'Protein (link to PDBe)'},
            {title: 'Q-score', width: '80px'},
            {title: 'RMSD', width: '80px'},
            {title: 'Aligned res.', width: '100px'},
            {title: 'Seq. identity', width: '100px'},
            {title: 'Alignment', 'searchable': false, 'orderable': false, width: '80px'},
        ],
        searching: false,
        paging: false,
        scrollCollapse: true,
        scrollResize: true,
        scrollY: 100,
        info: false,
        language: {
            emptyTable: 'No similar protein chains found in the database.'
        }
    });

    (function worker() {
        $.ajax({
            url: `/get_results?${object_params.toString()}`,
            success: function (data) {
                let idx = 0;
                statusTable.clear().draw();
                let phases_done = 0;
                let last_phase = '';
                if (data.hasOwnProperty('sketches_small_statistics')) {
                    last_phase = 'sketches_small';
                    phases_done++;
                    const small = data['sketches_small_statistics'];
                    statusTable.row.add([
                        '<b>Sketches small</b>',
                        '🗸',
                        `${small['pivot_dist_count']} (computed: ${small['pivot_dist_count'] - small['pivot_dist_cached']}, cached: ${small['pivot_dist_cached']})`,
                        format_time(small['pivot_dist_time']),
                        '-',
                        format_time(small['search_dist_time']),
                        `<b>${format_time(small['pivot_dist_time'] + small['search_dist_time'])}</b>`
                    ]).draw();
                } else {
                    statusTable.row.add([
                        '<b>Sketches small</b>',
                        '<div class="spinner-border spinner-border-sm" role="status"></div>',
                        '', '', '', '', ''
                    ]).draw();
                }

                if (data.hasOwnProperty('sketches_large_statistics')) {
                    last_phase = 'sketches_large';
                    phases_done++;
                    const large = data['sketches_large_statistics'];
                    statusTable.row.add([
                        '<b>Sketches large</b>',
                        '🗸',
                        `${large['pivot_dist_count']} (computed: ${large['pivot_dist_count'] - large['pivot_dist_cached']}, cached: ${large['pivot_dist_cached']})`,
                        format_time(large['pivot_dist_time']),
                        '-',
                        format_time(large['search_dist_time']),
                        `<b>${format_time(large['pivot_dist_time'] + large['search_dist_time'])}</b>`
                    ]).draw();
                } else {
                    statusTable.row.add([
                        '<b>Sketches large</b>',
                        '<div class="spinner-border spinner-border-sm" role="status"></div>',
                        '', '', '', '', ''
                    ]).draw();
                }

                if (data.hasOwnProperty('full_statistics')) {
                    last_phase = 'full';
                    phases_done++;
                    const full = data['full_statistics'];
                    statusTable.row.add([
                        '<b>PPP codes + sketches</b>',
                        '🗸',
                        `${full['pivot_dist_count']} (computed: ${full['pivot_dist_count'] - full['pivot_dist_cached']}, cached: ${full['pivot_dist_cached']})`,
                        format_time(full['pivot_dist_time']),
                        `${full['search_dist_count']} (computed: ${full['search_dist_count'] - (full['all_dist_cached'] - full['pivot_dist_cached'])}, cached: ${full['all_dist_cached'] - full['pivot_dist_cached']})`,
                        format_time(full['search_dist_time'] - full['pivot_dist_time']),
                        `<b>${format_time(full['search_dist_time'])}</b>`
                    ]).draw();
                } else {
                    statusTable.row.add([
                        '<b>PPP codes + sketches</b>',
                        '<div class="spinner-border spinner-border-sm" role="status"></div>',
                        '', '', '', '', ''
                    ]).draw();
                }

                let $displayed_phase = $('#displayed_phase');
                const displayed_phase = $displayed_phase.val();
                if (last_phase !== displayed_phase) {
                    resultsTable.clear().draw();
                    $displayed_phase.val(last_phase);
                }

                statusTable.columns.adjust().draw();

                // Remove rows that had ? but now are not present in results (they are below Q-score limit)
                let to_remove = [];
                resultsTable.rows().every(function () {
                    if (!(this.node().id in data['statistics'])) {
                        to_remove.push(this.node());
                    }
                });
                to_remove.forEach(function (node) {
                    resultsTable.row(node).remove().draw();
                });

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

                for (const res of data['statistics']) {
                    let [pdbid, chain] = res['object'].split(':');
                    let details_params = new URLSearchParams();
                    details_params.set('comp_id', comp_id);
                    details_params.set('object', res['object']);
                    details_params.set('chain', chain);
                    let name = localStorage.getItem(pdbid) === null ? '?' : localStorage.getItem(pdbid);

                    let qscore = '?';
                    let rmsd = '?';
                    let aligned = '?';
                    let seq_id = '?';

                    if (res['qscore'] !== -1) {
                        qscore = res['qscore'].toFixed(3);
                        rmsd = res['rmsd'].toFixed(3);
                        aligned = res['aligned'];
                        seq_id = res['seq_id'].toFixed(3);
                    }

                    const data = [idx + 1, res['object'],
                        `<a href="https://www.ebi.ac.uk/pdbe/entry/pdb/${pdbid}" target="_blank">
                            <div class="name_${pdbid}" style="max-width: 900px">${name}</div>
                        </a>`,
                        qscore, rmsd, aligned, seq_id,
                        `<a href="/details?${details_params.toString()}" target="_blank">Show</a>`];

                    if ($(`[id="${res['object']}"]`).length) {
                        resultsTable.row(`[id="${res['object']}"]`).data(data);
                    } else {
                        resultsTable.row.add(data).node().id = res['object'];
                    }
                    idx++;
                }
                resultsTable.columns.adjust().draw();

                if (phases_done !== 3 || data['status'] !== 'FINISHED') {
                    setTimeout(worker, 500);
                }
            }
        });
    })();
}


function load_molecule(plugin, comp_id, object, index) {

    let object_params = new URLSearchParams();
    object_params.set('comp_id', comp_id);
    object_params.set('object', object);

    const id = object === '_query' ? 'query' : object;

    plugin.loadMolecule({
        id: id,
        format: 'pdb',
        url: `/get_pdb?${object_params.toString()}`,
        modelRef: 'object-model' + index,
        doNotCreateVisual: true
    }).then(
        () => {
            let colors = LiteMol.Bootstrap.Immutable.Map();
            let style = {
                type: 'Cartoons',
                params: {detail: 'Automatic', showDirectionCone: false},
                theme: {
                    template: LiteMol.Bootstrap.Visualization.Molecule.Default.UniformThemeTemplate,
                    colors: colors.set('Uniform', LiteMol.Visualization.Molecule.Colors.DefaultPallete[index]),
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

    const parameters_string = window.location.search;
    const parameters = new URLSearchParams(parameters_string);
    const object = parameters.get('object');
    const comp_id = parameters.get('comp_id');

    load_molecule(plugin, comp_id, '_query', 0);
    load_molecule(plugin, comp_id, object, 1);
}


$(function () {
    let page = window.location.pathname;
    if (page === '/') {
        init_index();
    } else if (page === '/results') {
        init_results();
    } else if (page === '/details') {
        init_details();
    }
})
;
