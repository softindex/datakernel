import React from 'react';
import {BrowserRouter as Router, Route, Switch} from 'react-router-dom';
import MainScreen from "./pages/MainScreen/MainScreen";
import SignUp from "./pages/SignUp/SignUp";
import { withSnackbar } from 'notistack';

class App extends React.Component {
  render() {
    return (
      <Router>
        <Switch>
          <Route exact={true} path="/sign-up" component={SignUp}/>
          <Route path="/room/:roomId" component={MainScreen}/>
          <Route path="/" component={MainScreen}/>
        </Switch>
      </Router>
    );
  }
}

export default withSnackbar(App);
