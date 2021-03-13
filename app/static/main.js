'use strict';


function init_index() {
    let $file = $('#file');
    let $select = $('#select');
    let $input_type = $('#input-type');
    let $pdbid = $('#pdbid');

    $input_type.on('change', function () {
        if ($('input[name="input-type"]:checked').val() === 'file') {
            $file.prop('disabled', false);
            $select.prop('disabled', false);
            $pdbid.prop('disabled', true);
        } else {
            $file.prop('disabled', true);
            $select.prop('disabled', true);
            $pdbid.prop('disabled', false);
        }
    })

    $file.on('change', function () {
        if ($file.val()) {
            $select.prop('disabled', false);
        } else {
            $select.prop('disabled', true);
        }
    });

    $pdbid.on('input', function () {
        let val = $pdbid.val();
        if (val.length === 4) {
            $select.prop('disabled', false);
        } else {
            $select.prop('disabled', true);
        }
    })

    $input_type.trigger('change');
}


function fetch_name(name) {
    $.ajax({
        url: `https://www.ebi.ac.uk/pdbe/api/pdb/entry/summary/${name}`,
        success: function (data) {
            const title = data[name][0]['title'];
            localStorage[name] = title;
            $(`.name_${name}`).val(title);
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
        columnDefs : [
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
            {'title': 'No.'},
            {'title': 'Chain'},
            {'title': 'Name'},
            {'title': 'Q-score'},
            {'title': 'RMSD'},
            {'title': 'Aligned res.'},
            {'title': 'Seq. identity'},
            {'title': 'Alignment', 'searchable': false, 'orderable': false},
        ],
        searching: false,
        paging: false,
        scrollCollapse: true,
        scrollResize: true,
        scrollY: 100,
        info: false,
    });

    (function worker() {
        $.ajax({
            url: `/get_results?${object_params.toString()}`,
            success: function (data) {
                let idx = 0;
                statusTable.clear().draw();
                resultsTable.clear().draw();
                $('#table > tbody').empty();
                for (const res of data['statistics']) {
                    let [pdbid, chain] = res['object'].split(':');
                    pdbid = pdbid.toLowerCase();
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
                        qscore = res['qscore'];
                        rmsd = res['rmsd'];
                        aligned = res['aligned'];
                        seq_id = res['seq_id'];
                    }

                    resultsTable.row.add([idx + 1,
                        `<a href="https://www.ebi.ac.uk/pdbe/entry/pdb/${pdbid}" target="_blank">${res['object']}</a>`,
                        `<div class="name_${pdbid}">${name}</div>`,
                        qscore, rmsd, aligned, seq_id,
                        `<a href="/details?${details_params.toString()}" target="_blank">Show</a>`]).draw();

                    if (localStorage.getItem(pdbid) === null) {
                        fetch_name(pdbid);
                    }
                    idx++;
                }

                resultsTable.columns.adjust().draw();

                let phases_done = 0;
                if (data.hasOwnProperty('sketches_small_statistics')) {
                    phases_done++;
                    const small = data['sketches_small_statistics'];
                    statusTable.row.add([
                        '<b>Sketches small</b>',
                        '🗸',
                        small['pivot_dist_count'],
                        small['pivot_dist_time'],
                        small['search_dist_count'],
                        small['search_dist_time'],
                        `<b>${(small['pivot_dist_time'] + small['search_dist_time']).toLocaleString('cs-CZ')}`
                    ]).draw();
                } else {
                    statusTable.row.add([
                        '<b>Sketches small</b>',
                        '<div class="spinner-border spinner-border-sm" role="status"></div>',
                        '', '', '', '', ''
                    ]).draw();
                }

                if (data.hasOwnProperty('sketches_large_statistics')) {
                    phases_done++;
                    const large = data['sketches_large_statistics'];
                    statusTable.row.add([
                        '<b>Sketches large</b>',
                        '🗸',
                        large['pivot_dist_count'],
                        large['pivot_dist_time'],
                        large['search_dist_count'],
                        large['search_dist_time'],
                        `<b>${(large['pivot_dist_time'] + large['search_dist_time']).toLocaleString('cs-CZ')}`
                    ]).draw();
                } else {
                    statusTable.row.add([
                        '<b>Sketches large</b>',
                        '<div class="spinner-border spinner-border-sm" role="status"></div>',
                        '', '', '', '', ''
                    ]).draw();
                }

                if (data.hasOwnProperty('full_statistics')) {
                    phases_done++;
                    const full = data['full_statistics'];
                    statusTable.row.add([
                        '<b>PPP codes + sketches</b>',
                        '🗸',
                        full['pivot_dist_count'],
                        full['pivot_dist_time'],
                        full['search_dist_count'],
                        full['search_dist_time'] - full['pivot_dist_time'],
                        `<b>${full['search_dist_time'].toLocaleString('cs-CZ')}</b>`
                    ]).draw();
                } else {
                    statusTable.row.add([
                        '<b>PPP codes + sketches</b>',
                        '<div class="spinner-border spinner-border-sm" role="status"></div>',
                        '', '', '', '', ''
                    ]).draw();
                }

                statusTable.columns.adjust().draw();
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
