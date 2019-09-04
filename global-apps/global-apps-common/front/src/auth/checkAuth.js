import React from 'react';
import {Redirect, withRouter} from 'react-router-dom';
import connectService from '../connectService/connectService';
import AuthContext from './AuthContext';

function checkAuth(Component) {
  function CheckAuth(props) {
    const {authorized, location, ...otherProps} = props;

    if (location.pathname !== "/") {
      localStorage.setItem('redirectURI', location.pathname);
    }

    if (!authorized) {
      return <Redirect to='/sign-up'/>
    }

    return (
      <Component {...otherProps}/>
    );
  }

  return withRouter(connectService(AuthContext, ({authorized}) => ({authorized}))(CheckAuth));
}

export default checkAuth;
