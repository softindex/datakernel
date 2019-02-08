const d3 = require("d3-graphviz");
const $ = require("jquery");

const STATUS = $('#status');
const STATE = $('#state');
const OPS = $('#ops');
const TO_ADD = $('#toAdd');

$(document).ready(function () {
    $('#add').click(add);
    $('#sync').click(sync);

    setInterval(info, 500);
});

function add() {
    var toAdd = Math.floor(TO_ADD.val());
    if (toAdd === 0) {
        return;
    }
    TO_ADD.empty();
    $.ajax({
        type: "post",
        url: '/add',
        data: JSON.stringify(toAdd),
    }).then($ => good("Operation added"),
        error => bad("Failed to add", error));
}

function sync() {
    $.ajax({
        type: "get",
        url: '/sync',
    }).then($ => good("Successfully synced"),
        error => bad("Failed to sync", error));
}

function info() {
    $.getJSON("/info", json => {
        updateState(json[1]);
        updateOps(json[2]);
        updateGraph(json[3]);
    });
}

function updateState(state) {
    STATE.empty();
    STATE.append(state);
}

function updateOps(ops) {
    OPS.empty();
    ops.forEach(element => {
        OPS.append(element + ' ');
    });
}

function updateGraph(textGraphViz) {
    d3.graphviz("#graphviz")
        .zoom(false)
        .renderDot(textGraphViz);
}

function good(msg) {
    console.log(msg);
    STATUS
        .css('color', 'darkgreen')
        .html(msg);
}

function bad(msg, response) {
    STATUS
        .css('color', 'red')
        .html(msg + (response ? (response.responseText ? ': ' + response.responseText : ': code ' + response.status) : ''));
}
