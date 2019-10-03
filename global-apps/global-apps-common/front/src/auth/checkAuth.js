import React from 'react';
import {Redirect, withRouter} from 'react-router-dom';
import {connectService} from '../service/connectService';
import {AuthContext} from './AuthContext';

export function checkAuth(Component) {
  function CheckAuth({authorized, location, wasAuthorized, ...otherProps}) {
    if (!authorized) {
      if (location.pathname !== "/" && !wasAuthorized) {
        sessionStorage.setItem('redirectURI', location.pathname);
      }
      return <Redirect to='/sign-up'/>
    }

    return (
      <Component {...otherProps}/>
    );
  }

  return withRouter(connectService(AuthContext, ({authorized, wasAuthorized}) => ({
    authorized,
    wasAuthorized
  }))(CheckAuth));
}

