import React from 'react';
import {BrowserRouter, Route, Switch} from 'react-router-dom';
import MainScreen from './MainScreen/MainScreen';
import {OAuthCallback, SignUp} from 'global-apps-common';

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
        <Route exact={true} path="/sign-up/auth" component={OAuthCallback}/>
        <Route exact={true} path="/note/:noteId" component={MainScreen}/>
        <Route path="/" component={MainScreen}/>
      </Switch>
    </BrowserRouter>
  );
}

export default Router;
