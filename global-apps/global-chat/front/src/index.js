import React from 'react';
import ReactDOM from 'react-dom';
import App from './components/App/App';
import * as serviceWorker from './serviceWorker';
import cookies from 'js-cookie';
import {ClientOTNode, OTStateManager} from 'ot-core';
import roomsOTSystem from './modules/rooms/ot/RoomsOTSystem';
import contactsOTSystem from "./modules/contacts/ot/ContactsOTSystem";
import roomsSerializer from './modules/rooms/ot/serializer';
import contactsSerializer from './modules/contacts/ot/serializer';
import AccountService from './modules/account/AccountService';
import {MuiThemeProvider} from '@material-ui/core/styles';
import theme from "./components/themeConfig";
import CssBaseline from '@material-ui/core/CssBaseline';
import AccountContext from './modules/account/AccountContext';
import RoomsService from "./modules/rooms/RoomsService";
import RoomsContext from "./modules/rooms/RoomsContext";
import ContactsService from "./modules/contacts/ContactsService";
import ContactsContext from "./modules/contacts/ContactsContext";
import {SnackbarProvider} from "notistack";

const roomsOTNode = ClientOTNode.createWithJsonKey({
  url: '/ot/rooms',
  serializer: roomsSerializer
});

const contactsOTNode = ClientOTNode.createWithJsonKey({
  url: '/ot/contacts',
  serializer: contactsSerializer
});

const roomsOTStateManager = new OTStateManager(() => new Set(), roomsOTNode, roomsOTSystem);
const contactsOTStateManager = new OTStateManager(() => new Map(), contactsOTNode, contactsOTSystem);
const contactsService = new ContactsService(contactsOTStateManager);
const roomsService = new RoomsService(roomsOTStateManager, '/rooms', contactsService);
const accountService = new AccountService(cookies);
accountService.init();

ReactDOM.render((
  <MuiThemeProvider theme={theme}>
    <CssBaseline/>
    <RoomsContext.Provider value={roomsService}>
      <ContactsContext.Provider value={contactsService}>
        <AccountContext.Provider value={accountService}>
          <SnackbarProvider maxSnack={3}>
            <App/>
          </SnackbarProvider>
        </AccountContext.Provider>
      </ContactsContext.Provider>
    </RoomsContext.Provider>
  </MuiThemeProvider>
), document.getElementById('root'));

serviceWorker.unregister();
