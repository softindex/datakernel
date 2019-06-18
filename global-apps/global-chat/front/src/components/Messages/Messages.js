import React from 'react';
import { withStyles } from '@material-ui/core';
import messagesStyles from './messagesStyles';
import MessageItem from "./MessageItem"
import CircularProgress from '@material-ui/core/CircularProgress';
import Grow from '@material-ui/core/Grow';
import ChatRoomContext from '../../modules/chatroom/ChatRoomContext';
import connectService from '../../common/connectService';
import AccountContext from "../../modules/account/AccountContext";
import ContactsContext from "../../modules/contacts/ContactsContext";

class Messages extends React.Component {
  wrapper = React.createRef();

  componentDidUpdate(prevProps) {
    if (this.props.messages.length !== prevProps.messages.length) {
      this.wrapper.current.scrollTop = this.wrapper.current.scrollHeight;
    }
  }

  checkMessageAuthor = () => {
    this.props.contacts.map(contact => {
      if (contact.publicKey === this.props.publicKey) {
        return contact.name;
      }
    });
    return ''
  };

  render() {
    const {classes, ready, messages} = this.props;
    return (
      <div className={classes.root}>
        {!ready && (
          <Grow in={!ready}>
          <div className={classes.progressWrapper}>
            <CircularProgress/>
          </div>
          </Grow>
        )}
        {ready && (
          <div ref={this.wrapper} className={classes.wrapper}>
            {messages.map((message, index) => {
              const previousMessageAuthor = messages[index - 1] && messages[index - 1].authorPublicKey;
              let shape = 'start';
              if (previousMessageAuthor === message.author) {
                shape = 'medium';
              }
              return (
                <MessageItem
                  key={index}
                  text={message.content}
                  author={(message.author === this.props.publicKey) ? 'Me' : this.checkMessageAuthor}
                  time={new Date(message.timestamp).toLocaleString()}
                  loaded={message.loaded}
                  classes={classes}
                  drawSide={(message.author === this.props.publicKey) ? 'left' : 'right'}
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

export default withStyles(messagesStyles)(
  connectService(ContactsContext, ({ready, contacts}, contactsService) => ({ready, contacts, contactsService}))(
    connectService(ChatRoomContext, ({messages, ready}) => ({messages, ready}))(
      connectService(AccountContext, ({publicKey}) => ({publicKey}))(
        Messages
      )
    )
  )
);
