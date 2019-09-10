import React from 'react';
import {BrowserRouter, Route, Switch} from 'react-router-dom';
import MainScreen from "./MainScreen/MainScreen";
import {OAuthCallback, SignUp} from "global-apps-common";

function Router() {
  return (
    <BrowserRouter>
      <Switch>
        <Route
          exact={true}
          path="/sign-up"
          component={() => (
            <SignUp
              title="Global Chat"
              description="An application that allows you to create, edit and share documents.
              It is easy to manage, and synchronize on all devices."
            />
          )}
        />
        <Route exact={true} path="/sign-up/auth" component={OAuthCallback}/>
        <Route exact={true} path="/document/:documentId" component={MainScreen}/>
        <Route path="/" component={MainScreen}/>
      </Switch>
    </BrowserRouter>
  );
}

export default Router;
