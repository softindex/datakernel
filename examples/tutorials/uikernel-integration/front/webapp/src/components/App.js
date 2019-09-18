'use strict';

const React = require('react');
const GridComponent = require('./Grid');
const Popups = require('../common/popup/Popups');
const createClass = require('create-react-class');

const AppComponent = createClass({
  render: function () {
    return (
      <div>
        <h1 className="text-center">Users Grid</h1>
        <GridComponent/>
        <Popups/>
      </div>
    );
  }
});

module.exports = AppComponent;
