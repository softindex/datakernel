import React, {useEffect} from 'react';
import qs from 'query-string';
import connectService from '../../connectService/connectService';
import {Redirect, withRouter} from 'react-router-dom';
import AuthContext from "../AuthContext";

function OAuthCallback({location, authByPrivateKey}) {
  const params = qs.parse(location.search);

  useEffect(() => {
    if (params.privateKey) {
      authByPrivateKey(params.privateKey);
    }
  });

  return <Redirect to='/'/>;
}

export default connectService(
  AuthContext, (state, accountService) => ({
    authByPrivateKey(privateKey) {
      accountService.authByPrivateKey(privateKey);
    }
  })
)(
  withRouter(OAuthCallback)
);
