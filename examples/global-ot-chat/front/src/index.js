import React from 'react';
import ReactDOM from 'react-dom';
import App from './components/App';
import * as serviceWorker from './serviceWorker';
import ChatService from './modules/chat/ChatService';
import {OTStateManager, ClientOTNode} from 'ot-core';
import chatOTSystem from './modules/chat/ot/chatOTSystem';
import serializer from './modules/chat/ot/serializer';

const chatOTNode = ClientOTNode.createWithJsonKey({
  url: '/node',
  serializer
});
const chatOTStateManager = new OTStateManager(() => new Set(), chatOTNode, chatOTSystem);
const chatService = new ChatService(chatOTStateManager);

ReactDOM.render(<App chatService={chatService}/>, document.getElementById('root'));
serviceWorker.register();
