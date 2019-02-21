import React from 'react';
import {Graphviz} from 'graphviz-react';
import Paper from '@material-ui/core/Paper';
import {withStyles} from '@material-ui/core';
import commitsGraphStyles from './commitsGraphStyle';
import connectService from '../../common/connectService';
import ChatContext from '../../modules/chat/ChatContext';

class CommitsGraph extends React.Component {
  render() {
    if (this.props.commitsGraph) {
      return (
        <div className={this.props.classes.column}>
          <div className={this.props.classes.headerPadding}/>
          <Paper className={this.props.classes.root} elevation={1}>
            <Graphviz
              dot={this.props.commitsGraph}
              options={{
                fade: true,
                width: 300,
                zoom: true,
                height: window.innerHeight - 128,
              }}
            />
          </Paper>
        </div>
      )
    }
    return null;
  }
}

export default withStyles(commitsGraphStyles)(connectService(ChatContext, ({commitsGraph}) => ({
  commitsGraph
}))(CommitsGraph));

