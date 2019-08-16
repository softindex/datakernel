import React from 'react';
import ReactDOM from 'react-dom';
import App from './components/App';
import * as serviceWorker from './serviceWorker';
import cookies from 'js-cookie';
import AuthService from './modules/auth/AuthService';
import {MuiThemeProvider} from '@material-ui/core/styles';
import theme from "./components/themeConfig/themeConfig";
import CssBaseline from '@material-ui/core/CssBaseline';
import AuthContext from './modules/auth/AuthContext';
import {SnackbarProvider} from "notistack";

const accountService = new AuthService(process.env.REACT_APP_GLOBAL_OAUTH_LINK, cookies);
accountService.init();

ReactDOM.render((
  <MuiThemeProvider theme={theme}>
    <CssBaseline/>
    <SnackbarProvider maxSnack={1}>
      <AuthContext.Provider value={accountService}>
        <App/>
      </AuthContext.Provider>
    </SnackbarProvider>
  </MuiThemeProvider>
), document.getElementById('root'));

serviceWorker.unregister();
