import React from 'react';
import {BrowserRouter, Route, Switch} from 'react-router-dom';
import MainScreen from './MainScreen/MainScreen';
import SignUp from './SignUp/SignUp';
import AccountCallback from './SignUp/OAuthCallback';
import DebugScreen from './DebugScreen/DebugScreen';

function App() {
  return (
    <BrowserRouter>
      <Switch>
        <Route exact={true} path="/sign-up" component={SignUp}/>
        <Route exact={true} path="/sign-up/oauth" component={AccountCallback}/>
        <Route exact={true} path="/note/:noteId" component={MainScreen}/>
        <Route exact={true} path="/debug" component={DebugScreen}/>
        <Route exact={true} path="/debug/:noteId" component={DebugScreen}/>
        <Route path="/" component={MainScreen}/>
      </Switch>
    </BrowserRouter>
  );
}

export default App;
