import React, {useEffect} from 'react';
import qs from 'query-string';
import connectService from '../../connectService/connectService';
import {withRouter} from 'react-router-dom';
import AuthContext from "../AuthContext";
import AfterAuthRedirect from "../AfterAuthRedirect";

function OAuthCallback({location, authByPrivateKey}) {
  const params = qs.parse(location.search);

  useEffect(() => {
    if (params.privateKey) {
      authByPrivateKey(params.privateKey);
    }
  });

  return <AfterAuthRedirect/>;
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
