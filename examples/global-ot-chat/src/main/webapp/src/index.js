var chat = require('./chat');
var $ = require('jquery');
var nameRegex = /^[a-zA-Z0-9/_]+$/;

$('form').submit(function(e){
    e.preventDefault();
    validateUsername()
});

function validateUsername() {
    var username = $('#username').val();
    console.log(username);

    if (username.match(nameRegex) != null) {
        if (username.length <= 8) {
            window.location.replace('/chat/' + username);
        } else {
            alert("Username is not valid. Maximum 8 characters allowed.");
        }
    } else {
        alert("Username is not valid. Only characters letters, numbers and '_' are  acceptable.");
    }
};
