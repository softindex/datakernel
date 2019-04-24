import React from 'react';
import {Redirect} from 'react-router-dom';
import connectService from '../../common/connectService';
import AuthContext from '../../modules/auth/AuthContext';

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

  return connectService(AuthContext, ({authorized}) => ({authorized}))(CheckAuth);
}

export default checkAuth;
