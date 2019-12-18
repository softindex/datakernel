import React, {useEffect} from 'react';
import qs from 'query-string';
import connectService from '../../common/connectService';
import AuthContext from '../../modules/auth/AuthContext';
import {withRouter} from 'react-router-dom';
import AfterAuthRedirect from "../AfterAuthRedirect";

function AuthCallback({location, authByToken, authorized}) {
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
  withRouter(AuthCallback)
);

