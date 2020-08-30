'use strict';


function init_index() {
    let $file = $('#file');
    let $upload = $('#upload');

    $file.on('change', function () {
        if ($file.val()) {
            $upload.attr('disabled', false);
        } else {
            $upload.attr('disabled', true);
        }
    });
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
    } else if (page === '/details') {
        init_details();
    }
});
