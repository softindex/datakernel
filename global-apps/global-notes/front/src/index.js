import React from 'react';
import ReactDOM from 'react-dom';
import {SnackbarProvider} from 'notistack';
import {MuiThemeProvider} from '@material-ui/core/styles';
import CssBaseline from '@material-ui/core/CssBaseline';
import Router from './components/Router';
import * as serviceWorker from './serviceWorker';
import {AuthService, AuthContext} from 'global-apps-common';
import theme from './components/themeConfig/themeConfig';

const accountService = AuthService.create({
  appStoreURL: process.env.REACT_APP_GLOBAL_OAUTH_LINK,
  sessionIdField: process.env.REACT_APP_SESSION_ID_FIELD
});
accountService.init();

ReactDOM.render((
  <MuiThemeProvider theme={theme}>
    <CssBaseline/>
    <SnackbarProvider maxSnack={5}>
      <AuthContext.Provider value={accountService}>
        <Router/>
      </AuthContext.Provider>
    </SnackbarProvider>
  </MuiThemeProvider>
), document.getElementById('root'));

serviceWorker.unregister();
