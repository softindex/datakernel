import React, {useMemo} from 'react';
import {withStyles} from '@material-ui/core';
import chatStyles from './chatRoomStyles';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import ChatRoomService from "../../modules/chatroom/ChatRoomService";
import {RegisterDependency, initService, useSnackbar, getInstance} from "global-apps-common";
import CallsService from '../../modules/calls/CallsService';

function ChatRoom({roomId, publicKey, classes}) {
  const {showSnackbar} = useSnackbar();
  const callsService = getInstance(CallsService);
  const chatRoomService = useMemo(() => (
    ChatRoomService.createFrom(roomId, publicKey, callsService)
  ), [roomId, publicKey, callsService]);

  initService(chatRoomService, err => showSnackbar(err.message, 'error'));

  return (
    <RegisterDependency name={ChatRoomService} value={chatRoomService}>
      <div className={classes.root}>
        <Messages publicKey={publicKey}/>
        <MessageForm publicKey={publicKey}/>
      </div>
    </RegisterDependency>
  );
}

export default withStyles(chatStyles)(ChatRoom);
