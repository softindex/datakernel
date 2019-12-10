import React, {useState} from 'react';
import {withSnackbar} from 'notistack';
import Paper from '@material-ui/core/Paper';
import SendIcon from '@material-ui/icons/Send';
import PhoneIcon from '@material-ui/icons/Phone';
import messageFormStyles from './messageFormStyles';
import {withStyles} from '@material-ui/core';
import InputBase from '@material-ui/core/InputBase';
import Divider from '@material-ui/core/Divider';
import IconButton from '@material-ui/core/IconButton';
import {getInstance, useService} from 'global-apps-common';
import ChatRoomService from '../../modules/chatroom/ChatRoomService';

function MessageFormView({classes, message, onChangeMessage, onSubmit, joiningCall, onCall}) {
  return (
    <form className={classes.form} onSubmit={onSubmit}>
      <Paper className={classes.root} elevation={2}>
        <InputBase
          inputProps={{
            className: classes.inputText,
            required: true
          }}
          className={classes.input}
          placeholder="Message"
          onChange={onChangeMessage.bind(this)}
          value={message}
        />
        <Divider className={classes.divider}/>
        <IconButton color="primary" type="submit">
          <SendIcon/>
        </IconButton>
        <IconButton color="primary" onClick={onCall} disabled={joiningCall}>
          <PhoneIcon/>
        </IconButton>
      </Paper>
    </form>
  );
}

function MessageForm({classes, enqueueSnackbar}) {
  const chatRoomService = getInstance(ChatRoomService);
  const {joiningCall} = useService(chatRoomService);
  const [message, setMessage] = useState('');

  const props = {
    classes,
    message,
    joiningCall,
    onChangeMessage(event) {
      setMessage(event.target.value);
    },
    onSubmit(event) {
      event.preventDefault();
      if (!message) {
        return;
      }
      setMessage('');
      chatRoomService.sendMessage(message)
        .catch((err) => {
          enqueueSnackbar(err.message, {
            variant: 'error'
          });
        });
    },
    onCall() {
      chatRoomService.startCall()
        .catch(err => {
          enqueueSnackbar(err.message, {
            variant: 'error'
          });
        });
    }
  };

  return <MessageFormView {...props}/>
}

export default withSnackbar(withStyles(messageFormStyles)(MessageForm));
