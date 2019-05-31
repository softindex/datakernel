import React from 'react';
import {withStyles} from '@material-ui/core';
import chatStyles from './chatRoomStyles';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import chatRoomSerializer from "../../modules/chatroom/ot/serializer";
import chatRoomOTSystem from "../../modules/chatroom/ot/ChatRoomOTSystem";
import ChatRoomService from "../../modules/chatroom/ChatRoomService";

class ChatRoom extends React.Component {
  constructor(props) {
    super(props);
    let chatRoomOTNode = ClientOTNode.createWithJsonKey({
      url: '/index',
      serializer: chatRoomSerializer
    });
    let chatRoomStateManager = new OTStateManager(() => new Set(), chatRoomOTNode, chatRoomOTSystem);
    this._chatRoomService = new ChatRoomService(chatRoomStateManager);
  }

  componentDidMount() {
    this._chatRoomService.addChangeListener(this.update);
    this._chatRoomService.init();
  }

  componentWillUnmount() {
    this._chatRoomService.removeChangeListener(this.update);
  }

  update = newState => this.setState(newState);

  render() {
    let classes = this.props.classes;
    return (
      <div className={classes.root}>
        <div className={classes.headerPadding}/>
        <Messages ready={this._chatRoomService.ready} messages={this._chatRoomService.messages}/>
        <MessageForm/>
      </div>
    );
  }
}

export default withStyles(chatStyles)(ChatRoom);
