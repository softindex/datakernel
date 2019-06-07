import React from 'react';
import {withStyles} from '@material-ui/core';
import chatStyles from './chatRoomStyles';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import chatRoomSerializer from "../../modules/chatroom/ot/serializer";
import chatRoomOTSystem from "../../modules/chatroom/ot/ChatRoomOTSystem";
import ChatRoomService from "../../modules/chatroom/ChatRoomService";
import ChatRoomContext from '../../modules/chatroom/ChatRoomContext';

class ChatRoom extends React.Component {
  constructor(props) {
    super(props);
    let chatRoomOTNode = ClientOTNode.createWithJsonKey({
      url: '/index',
      serializer: chatRoomSerializer
    });
    let chatRoomStateManager = new OTStateManager(() => new Set(), chatRoomOTNode, chatRoomOTSystem);
    this.chatRoomService = new ChatRoomService(chatRoomStateManager);
  }

  componentDidMount() {
    this.chatRoomService.init();
  }

  update = newState => this.setState(newState);

  render() {
    let classes = this.props.classes;
    return (
      <ChatRoomContext.Provider value={this.chatRoomService}>
        <div className={classes.root}>
          <div className={classes.headerPadding}/>
          <Messages/>
          <MessageForm/>
        </div>
      </ChatRoomContext.Provider>
    );
  }
}

export default withStyles(chatStyles)(ChatRoom);
