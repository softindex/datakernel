var $ = require("jquery");

var d3 = require("d3-graphviz");

var lastUpdate = [];
var author = window.location.pathname.substr(6);

$(document).ready(function () {
    if (author === '') {
        return;
    }
    $('#delete').click(deleteMessage);
    $('#input').keydown(function (e) {
        if (e.keyCode == 13) {
            consumeInput();
        }
    })

    var updateTime = $('#time');
    updateTime.change(function (e) {
        changeUpdateInterval(updateTime.val());
    })

    var chat = $('#chat');
    chat.click(function (e) {
        var content = chat.val();
        var caretPos = chat[0].selectionStart;
        var lineBreak1 = content.lastIndexOf('\n', caretPos - 1);
        if (lineBreak1 == -1) {
            lineBreak1 = 0;
        }
        var lineBreak2 = content.indexOf('\n', caretPos);
        setSelectionRange(chat[0], lineBreak1, lineBreak2);
    });

    setInterval(update, 500);
});

function setSelectionRange(input, selectionStart, selectionEnd) {
    if (input.setSelectionRange) {
        input.focus();
        input.setSelectionRange(selectionStart, selectionEnd);
    }
    else if (input.createTextRange) {
        var range = input.createTextRange();
        range.collapse(true);
        range.moveEnd('character', selectionEnd);
        range.moveStart('character', selectionStart);
        range.select();
    }
}

function update() {
    $.ajax({
        type: "get",
        dataType: 'json',
        url: '/api/update',
        success: function (data) {
            lastUpdate = data;
            renderChat();
        },
        error: function (xhr, error) {
            alert('Failed to update: ' + xhr.responseText);
        }
    });

    $.ajax({
        type: "get",
        dataType: 'text',
        url: '/api/graph',
        success: function (data) {
            d3.graphviz("#graphviz")
                .zoom(false)
                .renderDot(data);
        }
    })
}

function send(message) {
    $.post('/api/send', { author: author, content: message });
}

function sendDelete(timestamp, message) {
    $.post('/api/delete', { author: author, timestamp: timestamp, content: message });
}

function renderChat() {
    var chat = $('#chat');
    var chatContent = '';
    for (var i = 0; i < lastUpdate.length; i++) {
        var time = timeConverter(lastUpdate[i][0]);
        var author = lastUpdate[i][1];
        var content = lastUpdate[i][2];
        chatContent += time + ' : ' + author + ': ' + content + '\n';
    }

    if (lastUpdate.length > 0) {
        chatContent = chatContent.substr(0, chatContent.length - 1);
    }

    chat.val(chatContent);
    chat.scrollTop(chat[0].scrollHeight);
}

function timeConverter(timestamp) {
    var date = new Date(timestamp);
    var hours = date.getHours();
    var minutes = "0" + date.getMinutes();
    var seconds = "0" + date.getSeconds();
    return hours + ':' + minutes.substr(-2) + ':' + seconds.substr(-2);
}

function consumeInput() {
    var input = $('#input');
    var content = input.val();
    input.val('');
    if (content === '') {
        return;
    }
    send(content);
}

function deleteMessage() {
    var input = $('#chat');
    var chat = input[0];
    var line = chat.value.substr(0, chat.selectionEnd).split("\n").length;

    var toDelete = lastUpdate[line - 1];

    if (author != toDelete[1]) {
        alert('Cannot delete message of another author');
        return;
    }

    sendDelete(toDelete[0], toDelete[2]);
}
