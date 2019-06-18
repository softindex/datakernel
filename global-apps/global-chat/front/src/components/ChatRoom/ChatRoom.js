import React from 'react';
import {withStyles} from '@material-ui/core';
import PropTypes from 'prop-types';
import chatStyles from './chatRoomStyles';
import Messages from '../Messages/Messages';
import MessageForm from '../MessageForm/MessageForm';
import ChatRoomService from "../../modules/chatroom/ChatRoomService";
import ChatRoomContext from '../../modules/chatroom/ChatRoomContext';
import connectService from '../../common/connectService';
import AccountContext from '../../modules/account/AccountContext';

class ChatRoom extends React.Component {
  static propTypes = {
    roomId: PropTypes.string.isRequired
  };

  constructor(props) {
    super(props);
    this.chatRoomService = ChatRoomService.createFrom(props.roomId, props.publicKey);
  }

  componentDidMount() {
     this.chatRoomService.init();
  }

  componentWillUnmount() {
    this.chatRoomService.stop();
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

export default connectService(AccountContext, ({publicKey}) => ({publicKey}))(
  withStyles(chatStyles)(ChatRoom)
);
