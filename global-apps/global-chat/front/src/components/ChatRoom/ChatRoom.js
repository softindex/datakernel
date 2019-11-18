import React, {useMemo} from 'react';
import {withStyles} from '@material-ui/core';
import chatStyles from './chatRoomStyles';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import ChatRoomService from "../../modules/chatroom/ChatRoomService";
import {withSnackbar} from "notistack";
import {RegisterDependency, initService, getInstance} from "global-apps-common";
import CallsService from '../../modules/calls/CallsService';
import CallsValidationService from '../../modules/calls/CallsValidationService';

function ChatRoom({roomId, publicKey, enqueueSnackbar, classes}) {
  const callsService = getInstance(CallsService);
  const callsValidationService = getInstance(CallsValidationService);
  const chatRoomService = useMemo(() => (
    ChatRoomService.createFrom(roomId, publicKey, callsService, callsValidationService)
  ), [roomId, publicKey, callsService, callsValidationService]);

  initService(chatRoomService, err => enqueueSnackbar(err.message, {
    variant: 'error'
  }));

  return (
    <RegisterDependency name={ChatRoomService} value={chatRoomService}>
      <div className={classes.root}>
        <Messages publicKey={publicKey}/>
        <MessageForm publicKey={publicKey}/>
      </div>
    </RegisterDependency>
  );
}

export default withSnackbar(withStyles(chatStyles)(ChatRoom));
