import React from 'react';
import connectService from '../../common/connectService';
import EditorContext from '../../modules/editor/EditorContext';
import './CommitsGraph.css';

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
      <div className="commits-graph">
        <div ref={this.graph}/>
      </div>
    );
  }
}

export default connectService(
  EditorContext,
  ({commitsGraph}) => ({commitsGraph})
)(CommitsGraph)

