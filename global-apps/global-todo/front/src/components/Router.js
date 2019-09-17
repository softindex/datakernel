import React from 'react';
import {BrowserRouter, Route, Switch} from 'react-router-dom';
import MainScreen from "./MainScreen/MainScreen";
import {SignUp, OAuthCallback} from "global-apps-common";

function Router() {
  return (
    <BrowserRouter>
      <Switch>
        <Route
          exact={true}
          path="/sign-up"
          component={() => (
            <SignUp
              title="Global Todo List"
              description="An application that allows you to manage todo lists. It is distributed and supports
              synchronization across all your devices."
            />
          )}
        />
        <Route exact={true} path="/sign-up/auth" component={OAuthCallback}/>
        <Route exact={true} path="/:listId" component={MainScreen}/>
        <Route path="/" component={MainScreen}/>
      </Switch>
    </BrowserRouter>
  );
}

export default Router;
