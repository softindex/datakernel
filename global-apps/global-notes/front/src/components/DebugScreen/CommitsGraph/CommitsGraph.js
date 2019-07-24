import React from 'react';
import GraphModel from "../../../modules/GraphModel";
import commitsGraphStyles from './commitsGraphStyles';
import {withStyles} from '@material-ui/core';

const Viz = window.Viz;
const UPDATE_INTERVAL = 1000;

class CommitsGraph extends React.Component {
  constructor(props, context) {
    super(props, context);
    this.timerId = null;
  }

  graph = React.createRef();

  async componentDidMount() {
    await this.updateGraph(this.props.noteId);
    this.timerId = setInterval(() => this.updateGraph(this.props.noteId), UPDATE_INTERVAL);
  }

  componentWillUnmount() {
    clearInterval(this.timerId);
  }

  updateGraph = async noteId => {
    try {
      let rawGraph = await GraphModel.getGraph(noteId);
      let viz = new Viz();

      viz.renderSVGElement(rawGraph)
        .then(element => {
          this.graph.current.innerHTML = "";
          this.graph.current.appendChild(element);
        })
        .catch(() => {
          viz = new Viz();
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

