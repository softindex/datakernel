import React from 'react';
import {withStyles} from '@material-ui/core';
import chatStyles from './chatRoomStyles';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import ChatRoomService from "../../modules/chatroom/ChatRoomService";
import {withSnackbar} from "notistack";
import {RegisterDependency} from "global-apps-common/lib";

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
      <RegisterDependency name={ChatRoomService} value={this.state.chatRoomService}>
        <div className={this.props.classes.root}>
          <Messages publicKey={this.props.publicKey}/>
          <MessageForm/>
        </div>
      </RegisterDependency>
    );
  }
}

export default withSnackbar(withStyles(chatStyles)(ChatRoom));
