import React from 'react';
import {withStyles} from '@material-ui/core';
import chatStyles from './chatRoomStyles';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import ChatRoomService from "../../modules/chatroom/ChatRoomService";
import ChatRoomContext from '../../modules/chatroom/ChatRoomContext';
import {connectService} from 'global-apps-common';
import {AuthContext} from 'global-apps-common';
import {withSnackbar} from "notistack";

class ChatRoom extends React.Component {
  state = {
    roomId: null,
    chatRoomService: null
  };

  static getDerivedStateFromProps(props, state) {
    if (props.roomId !== state.roomId) {
      if (state.chatRoomService) {
        state.chatRoomService.stop();
      }

      const chatRoomService = ChatRoomService.createFrom(props.roomId, props.publicKey);
      chatRoomService.init()
        .catch(err => {
          props.enqueueSnackbar(err.message, {
            variant: 'error'
          });
        });

      return {
        roomId: props.roomId,
        chatRoomService
      };
    }
  }

  componentWillUnmount() {
    this.state.chatRoomService.stop();
  }

  render() {
    return (
      <ChatRoomContext.Provider value={this.state.chatRoomService}>
        <div className={this.props.classes.root}>
          <Messages/>
          <MessageForm/>
        </div>
      </ChatRoomContext.Provider>
    );
  }
}

export default connectService(AuthContext, ({publicKey}) => ({publicKey}))(
  withSnackbar(withStyles(chatStyles)(ChatRoom))
);
