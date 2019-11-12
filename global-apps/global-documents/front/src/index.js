import React from 'react';
import ReactDOM from 'react-dom';
import Router from './components/Router';
import * as serviceWorker from './serviceWorker';
import {MuiThemeProvider} from '@material-ui/core/styles';
import theme from "./components/themeConfig/themeConfig";
import CssBaseline from '@material-ui/core/CssBaseline';
import {AuthContext, AuthService} from 'global-apps-common';
import {SnackbarProvider} from "notistack";

const accountService = AuthService.create({
  appStoreURL: process.env.REACT_APP_GLOBAL_OAUTH_LINK,
  sessionIdField: process.env.REACT_APP_SESSION_ID_FIELD
});
accountService.init();

ReactDOM.render((
  <MuiThemeProvider theme={theme}>
    <CssBaseline/>
    <SnackbarProvider maxSnack={1}>
      <AuthContext.Provider value={accountService}>
        <Router/>
      </AuthContext.Provider>
    </SnackbarProvider>
  </MuiThemeProvider>
), document.getElementById('root'));

serviceWorker.unregister();
