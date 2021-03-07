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

    $pdbid.on('input', function() {
        let val = $pdbid.val();
        if (val.length === 4) {
            $select.prop('disabled', false);
        }
        else {
            $select.prop('disabled', true);
        }
    })

    $input_type.trigger('change');
}


function init_results() {
    const parameters_string = window.location.search;
    const parameters = new URLSearchParams(parameters_string);

    let object_params = new URLSearchParams();
    const comp_id = parameters.get('comp_id');
    object_params.set('comp_id', comp_id);

    (function worker() {
        $.ajax({
            url: `/get_results?${object_params.toString()}`,
            success: function (data) {
                let idx = 0;
                console.log(data)
                $('#table > tbody').empty();
                for (const res of data['statistics']) {
                    let pdbid = res['object'].split(':')[0].toLowerCase();
                    let details_params = new URLSearchParams();
                    details_params.set('comp_id', comp_id);
                    details_params.set('object', res['object']);
                    details_params.set('chain', $('#chain').text());
                    let line = `<tr>
                                <td>${idx + 1}</td>
                                <td>${res['object']}</td>
                                <td><a href="https://www.ebi.ac.uk/pdbe/entry/pdb/${pdbid}" target="_blank"> ${pdbid}</a></td>
                                <td>${res['qscore']}</td>
                                <td>${res['rmsd']}</td>
                                <td>${res['aligned']}</td>
                                <td>${res['seq_id']}</td>
                                <td><a href="/details?${details_params.toString()}" target="_blank">Show alignment</a></td>
                                </tr>`
                    $('#table > tbody:last-child').append(line);
                    idx++;
                }
                let status = '';
                if (data['status'] === 'COMPUTING') {
                    setTimeout(worker, 500);
                    status = `<div class="spinner-border spinner-border-sm" role="status">
                                <span class="sr-only">Computing...</span>
                                </div>
                                Running phase: ${data['phase']} (done ${data['completed']} out of ${data['total']})`;
                } else {
                    if (data['statistics'].length <= 30) {
                        status = `Displaying ${data['statistics'].length} most similar structures`
                    } else {
                        status = `Displaying the first 30 most similar structures (out of ${data['similar']})`
                    }
                }
                $('#status').html(status);
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
