import React from 'react';
import Paper from '@material-ui/core/Paper';
import {withStyles} from '@material-ui/core';
import commitsGraphStyles from './commitsGraphStyle';
import connectService from '../../common/connectService';
import ChatContext from '../../modules/chat/ChatContext';

const Viz = window.Viz;

class CommitsGraph extends React.Component {
  graph = React.createRef();

  componentDidMount() {
    this.renderGraph();
  }

  componentDidUpdate() {
    this.renderGraph();
  }

  renderGraph() {
    let viz = new Viz();

    viz.renderSVGElement(this.props.commitsGraph)
      .then(element => {
        this.graph.current.innerHTML = '';
        this.graph.current.appendChild(element);
      })
      .catch(error => {
        viz = new Viz();
        console.error(error);
      });
  }

  render() {
    if (!this.props.commitsGraph) {
      return null;
    }

    return (
      <div className={this.props.classes.column}>
        <div className={this.props.classes.headerPadding}/>
        <Paper className={this.props.classes.root} elevation={1}>
          <div ref={this.graph}/>
        </Paper>
      </div>
    );
  }
}

export default withStyles(commitsGraphStyles)(
  connectService(ChatContext, ({commitsGraph}) => ({commitsGraph}))(CommitsGraph)
);

