import React from 'react';
import Viz from 'viz.js';
import {Module, render} from 'viz.js/lite.render.js';
import GraphModel from '../../../modules/GraphModel';
import commitsGraphStyles from './commitsGraphStyles';
import {withStyles} from '@material-ui/core';

const UPDATE_INTERVAL = 1000;

class CommitsGraph extends React.Component {
  constructor(props) {
    super(props);
    this.timerId = null;
    this.graph = React.createRef();
  }

  async componentDidMount() {
    await this.updateGraph(this.props.noteId);
    this.timerId = setInterval(() => this.updateGraph(this.props.noteId), UPDATE_INTERVAL);
  }

  componentWillUnmount() {
    clearInterval(this.timerId);
  }

  updateGraph = async noteId => {
    try {
      const rawGraph = await GraphModel.getGraph(noteId);
      let viz = new Viz({Module, render});

      viz.renderSVGElement(rawGraph)
        .then(element => {
          this.graph.current.innerHTML = '';
          this.graph.current.appendChild(element);
        })
        .catch(() => {
          viz = new Viz({Module, render});
        });
    } catch (e) {
      console.error(e);
    }
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

