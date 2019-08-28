import React from 'react';
import {BrowserRouter as Router, Route, Switch} from 'react-router-dom';
import MainScreen from "./MainScreen/MainScreen";
import {SignUp} from "global-apps-common";
import {withSnackbar} from 'notistack';
import {OAuthCallback} from "global-apps-common";

function App() {
  return (
    <Router>
      <Switch>
        <Route
          exact={true}
          path="/sign-up"
          component={() => (
            <SignUp
              title="Global Chat"
              description="An application that allows you to chats with your friends.
              It is easy to manage, and synchronize on all devices."
            />
          )}
        />
        <Route path="/sign-up/auth" component={OAuthCallback}/>
        <Route path="/room/:roomId" component={MainScreen}/>
        <Route path="/" component={MainScreen}/>
      </Switch>
    </Router>
  );
}

export default withSnackbar(App);
