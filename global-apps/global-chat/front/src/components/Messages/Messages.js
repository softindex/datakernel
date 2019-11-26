import React, {useLayoutEffect, useRef} from 'react';
import {withStyles} from '@material-ui/core';
import messagesStyles from './messagesStyles';
import MessageItem from "../MessageItem/MessageItem"
import CircularProgress from '@material-ui/core/CircularProgress';
import {getInstance, useService} from "global-apps-common";
import NamesService from "../../modules/names/NamesService";
import ChatRoomService from "../../modules/chatroom/ChatRoomService";

function MessagesView({classes, chatReady, messages, publicKey, names, namesReady}) {
  const wrapper = useRef();

  useLayoutEffect(() => {
    const currentScroll = wrapper.current;
    if (currentScroll) {
      currentScroll.scrollTop = currentScroll.scrollHeight;
    }
  }, [messages, names]); // Names are in deps because of dependency on wrapper-div scroll content on component init

  return (
    <div className={classes.root}>
      {(!chatReady || !namesReady) && (
        <div className={classes.progressWrapper}>
          <CircularProgress/>
        </div>
      )}
      {chatReady && namesReady && (
        <div ref={wrapper} className={classes.wrapper}>
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
                    names.get(message.authorPublicKey)
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

function Messages({classes, publicKey}) {
  const namesService = getInstance(NamesService);
  const {names, namesReady} = useService(namesService);
  const chatRoomService = getInstance(ChatRoomService);
  const {messages, chatReady} = useService(chatRoomService);

  const props = {
    classes,
    publicKey,
    names,
    namesReady,
    messages,
    chatReady
  };

  return <MessagesView {...props} />
}

export default withStyles(messagesStyles)(Messages);
