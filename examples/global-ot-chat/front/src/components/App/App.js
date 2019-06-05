import React from 'react';
import ChatContext from '../../modules/chat/ChatContext';
import {withStyles} from '@material-ui/core';
import appStyles from './appStyles';
import CommitsGraph from '../CommitsGraph/CommitsGraph';
import Header from '../Header/Header';
import Chat from '../Chat/Chat';
import connectService from '../../common/connectService';

class App extends React.Component {
  state = {
    isGraphOpen: false
  };

  componentDidMount() {
    this.props.chatService.init();
  }

  toggleGraph = () => {
    this.setState({
      isGraphOpen: !this.state.isGraphOpen
    });
  };

  render() {
    return (
      <>
        <Header onGraphToggle={this.toggleGraph}/>
        <div className={this.props.classes.root}>
          <Chat/>
          {this.state.isGraphOpen && <CommitsGraph/>}
        </div>
      </>
    );
  }
}

export default withStyles(appStyles)(
  connectService(ChatContext, (state, chatService) => ({chatService}))(App)
);
