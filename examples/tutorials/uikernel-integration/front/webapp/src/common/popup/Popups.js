'use strict';

const React = require('react');
const popupManager = require('./');
const createClass = require('create-react-class');

const Popups = createClass({
  getInitialState: function () {
    return {
      popups: popupManager.getAll()
    };
  },

  componentDidMount: function () {
    popupManager.addListener('change', function (popups) {
      this.setState({popups: popups});
    }.bind(this));
  },

  closeLast: function (e) {
    if (/(^| )modal($| )/.test(e.target.className)) {
      popupManager.closeLast();
    }
  },

  render: function () {
    const popups = this.state.popups.map(function (popup, key) {
      return (
        <div
          key={key}
          className='modal'
          onClick={this.closeLast}
        >
          <popup.Component {...popup.props}/>
        </div>
      );
    }.bind(this));

    return (
      <div>{popups}</div>
    );
  }
});

module.exports = Popups;
