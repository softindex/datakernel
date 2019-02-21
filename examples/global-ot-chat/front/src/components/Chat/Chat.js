import React from 'react';
import {withStyles} from '@material-ui/core';
import chatStyles from './chatStyles';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import LoginDialog from '../LoginDialog/LoginDialog';

class Chat extends React.Component {
  state = {
    login: 'John Doe'
  };

  onLoginChange = (login) => {
    this.setState({
      login
    })
  };

  render() {
    return (
      <div className={this.props.classes.root}>
        <div className={this.props.classes.headerPadding}/>
        <Messages login={this.state.login}/>
        <MessageForm login={this.state.login}/>
        <LoginDialog
          isOpen={!this.state.login}
          onSubmit={this.onLoginChange}
        />
      </div>
    );
  }
}

export default withStyles(chatStyles)(Chat);
