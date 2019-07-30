import React from 'react';
import Viz from 'viz.js';
import {Module, render} from 'viz.js/lite.render.js';
import GraphModel from '../../../modules/GraphModel';
import commitsGraphStyles from './commitsGraphStyles';
import {withStyles} from '@material-ui/core';

const UPDATE_INTERVAL = 1000;

class CommitsGraph extends React.Component {
  timerId = null;
  graph = React.createRef();

  async componentDidMount() {
    await this.updateGraph(this.props.noteId);
    this.timerId = setInterval(() => this.updateGraph(this.props.noteId), UPDATE_INTERVAL);
  }

  componentWillUnmount() {
    clearInterval(this.timerId);
  }

  updateGraph = async noteId => {
    const rawGraph = await GraphModel.getGraph(noteId);
    const viz = new Viz({Module, render});
    const element = await viz.renderSVGElement(rawGraph);
    this.graph.current.innerHTML = '';
    this.graph.current.appendChild(element);
  };

  render() {
    return (
      <div
        ref={this.graph}
        className={this.props.classes.root}
      />
    );
  }
}

export default withStyles(commitsGraphStyles)(CommitsGraph);

