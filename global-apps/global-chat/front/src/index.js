import React from 'react';
import ReactDOM from 'react-dom';
import Router from './components/Router';
import * as serviceWorker from './serviceWorker';
import cookies from 'js-cookie';
import AccountService from './modules/account/AccountService';
import {MuiThemeProvider} from '@material-ui/core/styles';
import theme from "./components/themeConfig";
import CssBaseline from '@material-ui/core/CssBaseline';
import AccountContext from './modules/account/AccountContext';
import {SnackbarProvider} from "notistack";

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
