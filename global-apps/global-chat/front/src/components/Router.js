import React from 'react';
import {BrowserRouter as ReactRouter, Route, Switch} from 'react-router-dom';
import MainScreen from "./MainScreen/MainScreen";
import {OAuthCallback, SignUp} from "global-apps-common";
import {withSnackbar} from 'notistack';
import InviteScreen from "./InviteScreen/InviteScreen";
import InitAuthorizedServices from './InitAuthorizedServices/InitAuthorizedServices';

function Router() {
  return (
    <ReactRouter>
      <Switch>
        <Route
          exact={true}
          path="/sign-up"
          component={() => (
            <SignUp
              title="Global Chat"
              description="An application that allows you to chat with your friends.
              It is easy to manage, and synchronize on all devices."
            />
          )}
        />
        <Route path="/sign-up/auth" component={OAuthCallback}/>
        <Route path="/*" component={() => (
          <InitAuthorizedServices>
            <Switch>
              <Route path="/room/:roomId" component={MainScreen}/>
              <Route path="/invite" component={InviteScreen}/>
              <Route path="/" component={MainScreen}/>
            </Switch>
          </InitAuthorizedServices>
        )}/>
      </Switch>
    </ReactRouter>
  );
}

export default withSnackbar(Router);
