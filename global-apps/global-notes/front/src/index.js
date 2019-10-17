import React from 'react';
import ReactDOM from 'react-dom';
import cookies from 'js-cookie';
import {SnackbarProvider} from 'notistack';
import {MuiThemeProvider} from '@material-ui/core/styles';
import CssBaseline from '@material-ui/core/CssBaseline';
import Router from './components/Router';
import * as serviceWorker from './serviceWorker';
import {AuthService, AuthContext} from 'global-apps-common';
import theme from './components/themeConfig/themeConfig';

const accountService = new AuthService(process.env.REACT_APP_GLOBAL_OAUTH_LINK, cookies, process.env.REACT_APP_SESSION_ID);
accountService.init();

ReactDOM.render((
  <MuiThemeProvider theme={theme}>
    <CssBaseline/>
    <SnackbarProvider maxSnack={2}>
      <AuthContext.Provider value={accountService}>
        <Router/>
      </AuthContext.Provider>
    </SnackbarProvider>
  </MuiThemeProvider>
), document.getElementById('root'));

serviceWorker.unregister();
