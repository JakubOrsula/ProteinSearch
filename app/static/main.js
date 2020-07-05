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



$(function () {
    let page = window.location.pathname;
    if (page === '/') {
        init_index();
    }
});
