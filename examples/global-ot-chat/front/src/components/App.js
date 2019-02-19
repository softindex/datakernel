import React from 'react';
import Messages from './Messages';
import MessageForm from './MessageForm';
import ChatContext from '../modules/chat/ChatContext';
import ChatService from '../modules/chat/ChatService';

const chatService = new ChatService();

function App() {
  return (
    <ChatContext.Provider value={chatService}>
      <Messages/>
      <MessageForm/>
    </ChatContext.Provider>
  );
}

export default App;
