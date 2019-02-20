import React from 'react';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import ChatContext from '../../modules/chat/ChatContext';
import ChatService from '../../modules/chat/ChatService';
import {withStyles} from '@material-ui/core';
import LoginDialog from '../LoginDialog/LoginDialog';
import appStyles from './AppStyles';
import theme from '../theme/themeConfig';
import {MuiThemeProvider} from '@material-ui/core/styles';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import TimelineIcon from '@material-ui/icons/Timeline';
import IconButton from '@material-ui/core/IconButton';
import Draggable from 'react-draggable';

const chatService = new ChatService();

class App extends React.Component {
  state = {
    login: null,
    isGraphOpen: false
  };

  onLoginChange = (login) => {
    this.setState({
      login
    })
  };

  componentDidMount() {
    this.props.chatService.init();
  }

  toggleGraph() {
    this.setState({
      isGraphOpen: !this.state.isGraphOpen
    })
  }

  render() {
    return (
      <MuiThemeProvider theme={theme}>
        <ChatContext.Provider value={this.props.chatService}>
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
          {/*<Grow in={!this.props.ready}>*/}
          {/*<div className={this.props.classes.progressWrapper}>*/}
          {/*<CircularProgress/>*/}
          {/*</div>*/}
          {/*</Grow>*/}
          <Draggable>
            <div className={this.props.classes.graphView}/>
          </Draggable>
          <div className={this.props.classes.root}>
            <Messages login={this.state.login}/>
            <MessageForm login={this.state.login}/>
            <LoginDialog
              isOpen={!this.state.login}
              onSubmit={this.onLoginChange}
            />
          </div>
        </ChatContext.Provider>
      </MuiThemeProvider>
    );
  }
}

export default withStyles(appStyles)(App);
