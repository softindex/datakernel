import React from 'react';
import {BrowserRouter, Route, Switch} from 'react-router-dom';
import MainScreen from "./MainScreen/MainScreen";
import {OAuthCallback, SignUp} from "global-apps-common";

function Router() {
  return (
    <BrowserRouter>
      <Switch>
        <Route exact={true} path="/sign-up" component={SignUp}/>
        <Route exact={true} path="/sign-up/oauth" component={OAuthCallback}/>
        <Route exact={true} path="/document/:documentId" component={MainScreen}/>
        <Route path="/" component={MainScreen}/>
      </Switch>
    </BrowserRouter>
  );
}

export default Router;
