import React from 'react';
import {withStyles} from '@material-ui/core';
import messagesStyles from './messagesStyles';
import MessageItem from "../MessageItem/MessageItem"
import CircularProgress from '@material-ui/core/CircularProgress';
import Grow from '@material-ui/core/Grow';
import {connectService} from 'global-apps-common';
import {AuthContext} from "global-apps-common";
import {getInstance, useService} from "global-apps-common/lib";
import NamesService from "../../modules/names/NamesService";
import ChatRoomService from "../../modules/chatroom/ChatRoomService";

class MessagesView extends React.Component {
  wrapper = React.createRef();

  componentDidUpdate(prevProps) {
    if (
      this.wrapper.current
      && this.props.messages.length !== prevProps.messages.length
    ) {
      this.wrapper.current.scrollTop = this.wrapper.current.scrollHeight;
    }
  }

  render() {
    const {classes, chatReady, messages, publicKey} = this.props;
    return (
      <div className={classes.root}>
        {!chatReady && (
          <Grow in={!chatReady}>
            <div className={classes.progressWrapper}>
              <CircularProgress/>
            </div>
          </Grow>
        )}
        {chatReady && (
          <div ref={this.wrapper} className={classes.wrapper}>
            {messages.map((message, index) => {
              const previousMessageAuthor = messages[index - 1] && messages[index - 1].authorPublicKey;
              let shape = 'start';
              if (previousMessageAuthor === message.authorPublicKey) {
                shape = 'medium';
              }
              return (
                <MessageItem
                  key={index}
                  text={message.content}
                  author={
                    message.authorPublicKey === publicKey ? '' :
                      this.props.names.get(message.authorPublicKey)
                  }
                  time={new Date(message.timestamp).toLocaleString()}
                  loaded={message.loaded}
                  drawSide={(message.authorPublicKey !== publicKey) ? 'left' : 'right'}
                  shape={shape}
                />
              )
            })}
          </div>
        )}
      </div>
    )
  }
}

function Messages(props) {
  const namesService = getInstance(NamesService);
  const {names} = useService(namesService);
  const chatRoomService = getInstance(ChatRoomService);
  const {messages, chatReady} = useService(chatRoomService);

  props = {
    ...props,
    names,
    messages,
    chatReady
  };

  return <MessagesView {...props} />
}

export default withStyles(messagesStyles)(
  connectService(
    AuthContext,
    ({publicKey}) => ({publicKey})
  )(
    Messages
  )
);
