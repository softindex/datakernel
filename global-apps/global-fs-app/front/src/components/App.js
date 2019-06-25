import React, {Component} from 'react';
import {BrowserRouter as Router, Route, Switch} from 'react-router-dom';
import cookies from 'js-cookie';
import MainScreen from './MainScreen';
import SignUp from './SignUp';
import AuthService from '../modules/auth/AuthService';
import AuthContext from '../modules/auth/AuthContext';
import Authorization from './Authorization';
import CssBaseline from '@material-ui/core/CssBaseline';
import {MuiThemeProvider} from '@material-ui/core/styles';
import theme from '../components/theme/themeConfig';
import AuthCallback from '../components/AuthCallback';

const authService = new AuthService(process.env.REACT_APP_AUTH_LINK, cookies, window.localStorage);
authService.init();

class App extends Component {
  render() {
    return (
      <AuthContext.Provider value={authService}>
        <CssBaseline/>
        <MuiThemeProvider theme={theme}>
          <Router>
            <Switch>
              <Route path="/folders/**" component={MainScreen}/>
              <Route path="/folders" component={MainScreen}/>
              <Route exact={true} path="/sign-up" component={SignUp}/>
              <Route exact={true} path="/sign-up/auth" component={AuthCallback}/>
              <Route exact={true} path="/" component={Authorization}/>
            </Switch>
          </Router>
        </MuiThemeProvider>
      </AuthContext.Provider>
    );
  }
}

export default App;
