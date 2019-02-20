import React from 'react';
import connectService from '../common/connectService';
import ChatContext from '../modules/chat/ChatContext';

function Messages({messages, ready}) {
  return (
    <>
      {!ready && <span>Loading...</span>}
      {messages.map((message, index) => (
        <React.Fragment key={index}>
          <b>{message.author}: </b>
          <span>({new Date(message.timestamp).toDateString()})</span>
          <div>{message.content}</div>
          <hr/>
        </React.Fragment>
      ))}
    </>
  );
}

export default connectService(ChatContext, ({messages, ready}) => ({messages, ready}))(Messages);
