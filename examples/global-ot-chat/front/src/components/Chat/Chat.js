import React from 'react';
import {withStyles} from '@material-ui/core';
import chatStyles from './chatStyles';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import LoginDialog from '../LoginDialog/LoginDialog';

function Chat({classes}) {
  return (
    <div className={classes.root}>
      <div className={classes.headerPadding}/>
      <Messages/>
      <MessageForm/>
      <LoginDialog/>
    </div>
  );
}

export default withStyles(chatStyles)(Chat);
