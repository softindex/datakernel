import React, {Component} from 'react';
import DocumentEditor from '../DocumentEditor/DocumentEditor';
import './App.css';
import {ClientOTNode, OTStateManager} from 'ot-core';
import serializer from '../../ot/serializer';
import editorOTSystem from '../../ot/editorOTSystem';

const otNode = ClientOTNode.createWithJsonKey({
  url: '/node',
  serializer
});
const otStateManager = new OTStateManager(() => '', otNode, editorOTSystem);

class App extends Component {
  state = {
    initialized: false
  };

  componentDidMount() {
    this.init();
  }

  async init() {
    try {
      await otStateManager.checkout();
      this.setState({
        initialized: true
      });
    } catch (e) {
      setTimeout(() => this.init(), 1000);
    }
  }

  onChange = ({initValue, changes}) => {
    if (initValue !== otStateManager.getState()) {
      return;
    }
    otStateManager.add(changes);
    otStateManager.sync();
  };

  render() {
    if (!this.state.initialized) {
      return 'Loading...';
    }

    return (
      <DocumentEditor
        value={{initValue: otStateManager.getState(), changes: []}}
        onChange={this.onChange}
      />
    );
  }
}

export default App;
