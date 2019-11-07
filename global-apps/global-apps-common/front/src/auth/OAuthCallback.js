import React, {useEffect} from 'react';
import qs from 'query-string';
import {connectService} from '../service/connectService';
import {withRouter} from 'react-router-dom';
import {AuthContext} from "./AuthContext";
import AfterAuthRedirect from "./AfterAuthRedirect";

function OAuthCallback({location, authByToken, authorized}) {
  const params = qs.parse(location.search);

  useEffect(() => {
    if (params.token) {
      authByToken(params.token);
    }
  }, []);

  if (authorized) {
    return <AfterAuthRedirect/>;
  }

  if (!params.token) {
    return 'No token received';
  }

  return null;
}

export default connectService(
  AuthContext, ({authorized}, accountService) => ({
    authByToken(token) {
      accountService.authByToken(token);
    },
    authorized
  })
)(
  withRouter(OAuthCallback)
);
