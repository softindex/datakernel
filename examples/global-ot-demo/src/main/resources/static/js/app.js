$(document).ready(function () {
    update();
    $('#add').click(add);
    $('#push').click(function () {
        execute('/push', 'post');
    });
    $('#merge').click(function () {
        execute('/merge', 'post');
    });
    $('#fetch').click(function () {
        doFetch('');
    });
    $('#commit').click(function () {
        execute('/commit', 'post');
    });
    $('#pull').click(function () {
        execute('/pull', 'get');
    });
    $('#rebase').click(function () {
        execute('/rebase', 'post');
    });
    $('#reset').click(function () {
        execute('/reset', 'post');
    })
    $('#newManager').click(openNew);
    $('#update').click(update);

    $('#updateGraph').click(updateGraph);
    document.title = '[' + $.urlParam('id') + ']' + ' OT State Manager';
});

function add() {
    var toAdd = Math.floor($('#toAdd').val());
    $.ajax({
        type: "put",
        dataType: 'json',
        url: '/add' + getId() + '&value=' + toAdd,
        success: function (data) {
            updateOps(data);
            updateState();
        },
        error: function (xhr, error) {
            alert('Failed to add commits: ' + error);
        }
    });
}

function updateOps(json) {
    $('#ops').empty();
    json.forEach(element => {
        $('#ops').append('[' + element + '] ');
    });
}

function updateState() {
    $.ajax({
        type: "get",
        dataType: 'json',
        url: '/state' + getId(),
        success: function (data) {
            $('#state').empty();
            $('#state').append(data);
        },
        error: function (xhr, error) {
            alert('Failed to update state: ' + error);
        }
    });
}

function checkout(id) {
    $.ajax({
        type: "get",
        url: '/checkout' + getId() + '&commitId=' + id,
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
            updateOps(json['diffs']);
            updateState();
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
    d3.select("#graph")
        .graphviz()
        .zoom(false)
        .renderDot(textGraphViz, updateCb);
}

function doFetch(id) {
    $.ajax({
        type: 'get',
        url: '/fetch' + getId() + '&commitId=' + id,
        success: function (data) {
            update();
        },
        error: function (xhr, error) {
            alert('Fetch failed: ' + error);
        }
    });
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

function updateCb() {
    $('.node').off("click");
    $('.node').click(function () {
        let id = $("g > a", this).attr('title');
        checkout(JSON.stringify(id));
    })
    $('.node').off("contextmenu");
    $('.node').contextmenu(function (event) {
        event.preventDefault();
        let id = $("g > a", this).attr('title');
        doFetch(JSON.stringify(id))
    });
}

