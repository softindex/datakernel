import React, {useEffect} from 'react';
import qs from 'query-string';
import {connectService} from '../service/connectService';
import {withRouter} from 'react-router-dom';
import {AuthContext} from "./AuthContext";
import AfterAuthRedirect from "./AfterAuthRedirect";

function OAuthCallback({location, authWithToken}) {
  const params = qs.parse(location.search);

  useEffect(() => {
    if (params.token) {
      authWithToken(params.token);
    } else {
      console.error('No token received');
    }
  });

  return <AfterAuthRedirect/>;
}

export default connectService(
  AuthContext, (state, accountService) => ({
    authWithToken(token) {
      accountService.authWithToken(token);
    }
  })
)(
  withRouter(OAuthCallback)
);
