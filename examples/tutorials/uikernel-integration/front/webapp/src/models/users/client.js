'use strict';

const UIKernel = require('uikernel');
const validator = require('./validator');
const xhr = require('xhr');

const API_URL = '/api/users';

const users = new UIKernel.Models.Grid.Xhr({
  api: API_URL,
  validator: validator
});

users.delete = function (recordId, cb) {
  xhr({
    method: 'DELETE',
    headers: {'Content-type': 'application/json'},
    uri: API_URL + '/' + recordId
  }, function (err) {
    cb(err);
  });
};

module.exports = users;
