import React from 'react';
import {BrowserRouter, Route, Switch} from 'react-router-dom';
import MainScreen from "./MainScreen/MainScreen";
import SignUp from "./SignUp/SignUp";
import AccountCallback from "./SignUp/AccountCallback";

function Router() {
  return (
    <BrowserRouter>
      <Switch>
        <Route exact={true} path="/sign-up" component={SignUp}/>
        <Route exact={true} path="/sign-up/oauth" component={AccountCallback}/>
        <Route exact={true} path="/:listId" component={MainScreen}/>
        <Route path="/" component={MainScreen}/>
      </Switch>
    </BrowserRouter>
  );
}

export default Router;
