import React from 'react';
import ChatContext from '../../modules/chat/ChatContext';
import {withStyles} from '@material-ui/core';
import appStyles from './AppStyles';
import theme from '../theme/themeConfig';
import {MuiThemeProvider} from '@material-ui/core/styles';
import CssBaseline from '@material-ui/core/CssBaseline';
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
    console.log(this.state.isGraphOpen)
    this.setState({
      isGraphOpen: !this.state.isGraphOpen
    });
  }

  render() {
    return (
      <MuiThemeProvider theme={theme}>
        <CssBaseline/>
        <ChatContext.Provider value={this.props.chatService}>
          <Header onGraphToggle={this.toggleGraph}/>
          <div className={this.props.classes.root}>
            <Chat/>
            {this.state.isGraphOpen && <CommitsGraph/>}
          </div>
        </ChatContext.Provider>
      </MuiThemeProvider>
    );
  }
}

export default withStyles(appStyles)(
  connectService(ChatContext, (state, chatService) => ({chatService}))(App)
);
