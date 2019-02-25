import React from 'react';
import ReactDOM from 'react-dom';
import App from './components/App/App';
import * as serviceWorker from './serviceWorker';
import ChatService from './modules/chat/ChatService';
import {OTStateManager, ClientOTNode} from 'ot-core';
import chatOTSystem from './modules/chat/ot/chatOTSystem';
import serializer from './modules/chat/ot/serializer';
import AccountService from './modules/account/AccountService';
import {MuiThemeProvider} from '@material-ui/core/styles';
import theme from "./components/themeConfig";
import CssBaseline from '@material-ui/core/CssBaseline';
import ChatContext from './modules/chat/ChatContext';
import AccountContext from './modules/account/AccountContext';
import GraphModel from './modules/chat/GraphModel';

const chatOTNode = ClientOTNode.createWithJsonKey({
  url: '/node',
  serializer
});
const graphModel = new GraphModel();
const chatOTStateManager = new OTStateManager(() => new Set(), chatOTNode, chatOTSystem);
const chatService = new ChatService(chatOTStateManager, graphModel);
const accountService = new AccountService();

ReactDOM.render((
  <MuiThemeProvider theme={theme}>
    <CssBaseline/>
    <ChatContext.Provider value={chatService}>
      <AccountContext.Provider value={accountService}>
        <App/>
      </AccountContext.Provider>
    </ChatContext.Provider>
  </MuiThemeProvider>
), document.getElementById('root'));

serviceWorker.register();
