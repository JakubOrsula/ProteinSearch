'use strict';


function init_index() {
    let $file = $('#file');
    let $chain = $('#chain');
    let $run = $('#run');

    $file.on('change', function () {
        if ($file.val() && $chain.val()) {
            $run.attr('disabled', false);
        } else {
            $run.attr('disabled', true);
        }
    });

    $chain.on('input', function () {
        if ($file.val() && $chain.val()) {
            $run.attr('disabled', false);
        } else {
            $run.attr('disabled', true);
        }
    });


}



$(function () {
    let page = window.location.pathname;
    if (page === '/') {
        init_index();
    }
});
