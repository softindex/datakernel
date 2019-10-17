import React, {useEffect, useState} from 'react';
import qs from 'query-string';
import connectService from '../../common/connectService';
import AuthContext from '../../modules/auth/AuthContext';
import {Redirect, withRouter} from 'react-router-dom';

function AuthCallback({location, authWithToken}) {
  const params = qs.parse(location.search);

  useEffect(() => {
    if (params.token) {
      authWithToken(params.token);
    } else {
      console.error('No token received');
    }
  });

  return <Redirect to='/'/>;
}

export default connectService(
  AuthContext, ({}, accountService) => ({
    authWithToken(token) {
      accountService.authWithToken(token);
    }
  })
)
(
  withRouter(AuthCallback)
);

