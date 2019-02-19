import React from 'react';
import connectService from '../common/connectService';
import ChatContext from '../modules/chat/ChatContext';

function Messages({messages}) {
  return (
    <>
      {messages.map((message, index) => (
        <React.Fragment key={index}>
          <b>{message.author}: </b>
          {message.loaded && <span>({message.time.toDateString()}) </span>}
          {!message.loaded && <span>(Loading) </span>}
          <div>{message.text}</div>
          <hr/>
        </React.Fragment>
      ))}
    </>
  );
}

export default connectService(ChatContext, ({messages}) => ({messages}))(Messages);
