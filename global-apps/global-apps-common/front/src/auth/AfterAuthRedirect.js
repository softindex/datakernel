import React, {useEffect} from 'react';
import path from 'path';
import {Redirect, withRouter} from 'react-router-dom';

function AfterAuthRedirect({history}) {
  const redirectURI = sessionStorage.getItem('redirectURI') || '/';

  useEffect(() => {
    if (redirectURI !== '/') {
      history.push(path.join(redirectURI));
      sessionStorage.removeItem('redirectURI');
    }
  }, [redirectURI]);

  return <Redirect to='/'/>;
}

export default withRouter(AfterAuthRedirect);
