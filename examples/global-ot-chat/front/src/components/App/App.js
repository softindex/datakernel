import React from 'react';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import {withStyles} from '@material-ui/core';
import LoginDialog from '../LoginDialog/LoginDialog';
import appStyles from './AppStyles';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import TimelineIcon from '@material-ui/icons/Timeline';
import IconButton from '@material-ui/core/IconButton';
import connectService from '../../common/connectService';
import ChatContext from '../../modules/chat/ChatContext';

class App extends React.Component {
  state = {
    isGraphOpen: false
  };

  componentDidMount() {
    this.props.chatService.init();
  }

  toggleGraph() {
    this.setState({
      isGraphOpen: !this.state.isGraphOpen
    });
  }

  render() {
    return (
      <>
        <AppBar position="absolute" color="default">
          <Toolbar>
            <Typography variant="h6" color="inherit">
              Global OT Chat
            </Typography>
            <div className={this.props.classes.grow}/>
            <IconButton
              onClick={this.toggleGraph}
              color="inherit"
            >
              <TimelineIcon/>
            </IconButton>
          </Toolbar>
        </AppBar>
        <div className={this.props.classes.root}>
          <Messages/>
          <MessageForm/>
          <LoginDialog/>
        </div>
      </>
    );
  }
}

export default withStyles(appStyles)(
  connectService(ChatContext, (state, chatService) => ({chatService}))(App)
);
