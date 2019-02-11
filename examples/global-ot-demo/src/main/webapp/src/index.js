const d3 = require("d3-graphviz");
const $ = require("jquery");

const STATUS = $('#status');
const STATE = $('#state');
const TO_ADD = $('#toAdd');

$(document).ready(function () {
    info();
    $('#add').click(add);

    $('#newManager').click(openNew);
    document.title = '[' + $.urlParam('id') + ']' + ' OT State Manager';
    setTimeout(5000, setInterval(info, 500));
});

function add() {
    var toAdd = Math.floor(TO_ADD.val());
    if (toAdd === 0) {
        return;
    }
    TO_ADD.empty();
    $.ajax({
        type: "post",
        url: '/add' + getId(),
        data: JSON.stringify(toAdd),
    }).then($ => good("Operation added", 'darkgreen'),
        error => bad("Failed to add", error));
}

function info() {
    fetch('/info' + getId(), {
        method: 'GET',
        dataType: 'json',
        credentials: 'include'
    })
        .then(function (res) {
            if (res.redirected) {
                window.location.href = res.url;
                return;
            }
            return res.json();
        }, error => bad("Failed to update", error))
        .then(function (json) {
            updateState(json[1]);
            updateStatus(json[2]);
            updateGraph(json[3]);
        });
}

function updateState(state) {
    STATE.empty();
    STATE.append(state);
}

function updateStatus(status) {
    if (status === 'Syncing') {
        good(status + '...', 'yellowgreen');
    } else if (status === 'Synced') {
        good(status, 'darkgreen');
    }
}

function updateGraph(textGraphViz) {
    d3.graphviz("#graphviz")
        .zoom(false)
        .renderDot(textGraphViz);
}

function getId() {
    return '?id=' + $.urlParam('id');
}

function openNew() {
    var win = window.open('/', '_blank');
    win.focus();
}

$.urlParam = function (name) {
    var results = new RegExp('[\?&]' + name + '=([^&#]*)').exec(window.location.href);
    if (results == null) {
        return '';
    }
    return decodeURI(results[1]) || 0;
}

function good(msg, color) {
    STATUS
        .css('color', color)
        .html(msg);
}

function bad(msg, response) {
    STATUS
        .css('color', 'red')
        .html(msg + (response ? (response.responseText ? ': ' + response.responseText : ': code ' + response.status) : ''));
}
