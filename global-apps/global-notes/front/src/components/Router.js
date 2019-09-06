import React from 'react';
import {BrowserRouter, Route, Switch} from 'react-router-dom';
import MainScreen from './MainScreen/MainScreen';
import {SignUp, AccountCallback} from 'global-apps-common';
import DebugScreen from './DebugScreen/DebugScreen';

function Router() {
  return (
    <BrowserRouter>
      <Switch>
        <Route exact={true} path="/sign-up" component={() => (
          <SignUp
            title="Global Notes"
            description="An application that allows you to keep notes. It is distributed and supports synchronization
              across all your devices."
          />
        )}/>
        <Route exact={true} path="/sign-up/oauth" component={AccountCallback}/>
        <Route exact={true} path="/note/:noteId" component={MainScreen}/>
        <Route exact={true} path="/debug" component={DebugScreen}/>
        <Route exact={true} path="/debug/:noteId" component={DebugScreen}/>
        <Route path="/" component={MainScreen}/>
      </Switch>
    </BrowserRouter>
  );
}

export default Router;
