import React, {Component} from 'react';
import {BrowserRouter as Router, Route, Switch} from 'react-router-dom';
import cookies from 'js-cookie';
import AuthService from '../modules/auth/AuthService';
import AuthContext from '../modules/auth/AuthContext';
import CssBaseline from '@material-ui/core/CssBaseline';
import AuthCallback from '../components/AuthCallback/AuthCallback';
import SignUp from "./SignUp/SignUp";
import MainScreen from "./MainScreen/MainScreen";
import MuiThemeProvider from "@material-ui/core/es/styles/MuiThemeProvider";
import {SnackbarProvider} from "notistack";
import theme from "./themeConfig";

const authService = new AuthService(process.env.REACT_APP_AUTH_LINK, cookies, window.localStorage);
authService.init();

class App extends Component {
  render() {
    return (
      <MuiThemeProvider theme={theme}>
        <SnackbarProvider maxSnack={1}>
          <AuthContext.Provider value={authService}>
            <CssBaseline/>
            <Router>
              <Switch>
                <Route exact={true} path="/sign-up" component={SignUp}/>
                <Route exact={true} path="/sign-up/oauth" component={AuthCallback}/>
                <Route path="/" component={MainScreen}/>
              </Switch>
            </Router>
          </AuthContext.Provider>
        </SnackbarProvider>
      </MuiThemeProvider>
    );
  }
}

export default App;
