import React from 'react';
import connectService from '../../common/connectService';
import EditorContext from '../../modules/editor/EditorContext';
import './CommitsGraph.css';
import throttle from 'lodash.throttle';

const Viz = window.Viz;
const THROTTLE_RENDER_GRAPH = 500;

class CommitsGraph extends React.Component {
  graph = React.createRef();

  componentDidMount() {
    this.renderGraph();
  }

  componentDidUpdate() {
    this.renderGraph();
  }

  renderGraph = throttle(() => {
    let viz = new Viz();

    viz.renderSVGElement(this.props.commitsGraph)
      .then(element => {
        this.graph.current.innerHTML = '';
        this.graph.current.appendChild(element);
      })
      .catch(() => {
        viz = new Viz();
      });
  }, THROTTLE_RENDER_GRAPH);

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

