const d3 = require("d3-graphviz");
const $ = require("jquery");

var localChanges = [];

$(document).ready(function () {
    update();
    $('#add').click(add);
    $('#reset').click(reset);
    $('#push').click(push);
    $('#merge').click(function () {
        execute('/merge', 'post');
    });
    $('#pull').click(function () {
        execute('/pull', 'get');
    });
    $('#checkout').click(checkout)
    $('#newManager').click(openNew);
    document.title = '[' + $.urlParam('id') + ']' + ' OT State Manager';
});

function add() {
    var toAdd = Math.floor($('#toAdd').val());
    if (toAdd === 0) {
        return;
    }
    localChanges.push(toAdd);
    updateOps()
}

function reset(){
    localChanges = [];
    updateOps();
}

function push() {
    if (localChanges.length === 0){
        return;
    }
    $.ajax({
        type: "post",
        url: '/push' + getId(),
        data: JSON.stringify(localChanges),
        success: function (data) {
            reset();
            update();
        },
        error: function (xhr, error) {
            alert('Failed to update state: ' + error);
        }
    });
}

function updateOps() {
    var ops = $('#ops');
    ops.empty();
    localChanges.forEach(element => {
        $('#ops').append('[' + (element > 0 ? '+' : '-') + element + '] ');
    });
}

function updateState(state) {
    $('#state').empty();
    $('#state').append(state);
}

function checkout() {
    $.ajax({
        type: "get",
        url: '/checkout' + getId(),
        success: function (data) {
            update();
        },
        error: function (xhr, error) {
            alert('Checkout failed: ' + error);
        }
    });
}

function execute(path, method) {
    $.ajax({
        type: method,
        url: path + getId(),
        success: function (data) {
            update();
        },
        error: function (xhr, error) {
            alert(path + ' request failed: ' + error);
        }
    });
}

function update() {
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
        })
        .then(function (json) {
            updateState(json[1]);
            updateGraph();
        });
}

function updateGraph() {
    fetch('/graph' + getId(), {
        method: 'GET'
    })
        .then(function (res) {
            return res.text();
        })
        .then(function (graph) {
            render(graph);
        });
}

function render(textGraphViz) {
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

