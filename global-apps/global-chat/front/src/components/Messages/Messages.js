import React from 'react';
import {withStyles} from '@material-ui/core';
import messagesStyles from './messagesStyles';
import MessageItem from "./MessageItem"
import CircularProgress from '@material-ui/core/CircularProgress';
import Grow from '@material-ui/core/Grow';
import ChatRoomContext from '../../modules/chatroom/ChatRoomContext';
import connectService from '../../common/connectService';

class Messages extends React.Component {
  wrapper = React.createRef();

  componentDidUpdate(prevProps) {
    if (this.props.messages.length !== prevProps.messages.length) {
      this.wrapper.current.scrollTop = this.wrapper.current.scrollHeight;
    }
  }

  render() {
    return (
      <div className={this.props.classes.root}>
        <Grow in={!this.props.ready}>
          <div className={this.props.classes.progressWrapper}>
            <CircularProgress/>
          </div>
        </Grow>
        <div ref={this.wrapper} className={this.props.classes.wrapper}>
          {this.props.messages.map((message, index) => {
            const previousMessageAuthor = this.props.messages[index - 1] && this.props.messages[index - 1].author;
            let shape = 'start';
            if (previousMessageAuthor === message.author) {
              shape = 'medium';
            }
            return (
              <MessageItem
                key={index}
                text={message.content}
                author={message.author}
                time={new Date(message.timestamp).toLocaleString()}
                loaded={message.loaded}
                classes={this.props.classes}
                drawSide={(message.author === this.props.login) ? 'left' : 'right'}
                shape={shape}
              />
            )
          })}
        </div>
      </div>
    )
  }
}

export default connectService(
  ChatRoomContext, ({messages, ready}) => ({messages, ready})
)(
  withStyles(messagesStyles)(Messages)
);
