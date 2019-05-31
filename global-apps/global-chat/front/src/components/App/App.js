import React from 'react';
import {BrowserRouter as Router, Route, Switch} from 'react-router-dom';
import MainScreen from "../MainScreen";
import SignUp from "../SignUp";
import Authorization from "../Authorization";
import Chat from "../ChatRoom/ChatRoom";

class App extends React.Component {
  render() {
    return <Router>
      <Switch>
        <Route path="/rooms" component={MainScreen}/>
        <Route path="/room/**" component={Chat}/>
        <Route exact={true} path="/sign-up" component={SignUp}/>
        <Route exact={true} path="/" component={Authorization}/>
      </Switch>
    </Router>;

  }
}

export default App;
