import React from 'react';
import ReactDOM from 'react-dom';
import cookies from 'js-cookie';
import {SnackbarProvider} from 'notistack';
import {MuiThemeProvider} from '@material-ui/core/styles';
import CssBaseline from '@material-ui/core/CssBaseline';
import Router from './components/Router';
import * as serviceWorker from './serviceWorker';
import AccountService from './modules/account/AccountService';
import theme from './components/themeConfig';
import AccountContext from './modules/account/AccountContext';


const accountService = new AccountService(process.env.REACT_APP_GLOBAL_OAUTH_LINK, cookies);
accountService.init();

ReactDOM.render((
  <MuiThemeProvider theme={theme}>
    <CssBaseline/>
    <SnackbarProvider maxSnack={1}>
      <AccountContext.Provider value={accountService}>
        <Router/>
      </AccountContext.Provider>
    </SnackbarProvider>
  </MuiThemeProvider>
), document.getElementById('root'));

serviceWorker.unregister();
