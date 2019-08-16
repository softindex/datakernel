import React, {useEffect} from 'react';
import qs from 'query-string';
import connectService from '../../common/connectService';
import {Redirect} from 'react-router-dom';
import AuthContext from "../../modules/auth/AuthContext";

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
  OAuthCallback
);