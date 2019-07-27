import React from 'react';
import {Redirect} from 'react-router-dom';
import connectService from './connectService';
import AccountContext from '../modules/account/AccountContext';

function checkAuth(Component) {
  function CheckAuth(props) {
    const {authorized, ...otherProps} = props;
    if (!authorized) {
      return <Redirect to='/sign-up'/>
    }

    return (
      <Component {...otherProps}/>
    );
  }

  return connectService(AccountContext, ({authorized}) => ({authorized}))(CheckAuth);
}

export default checkAuth;
