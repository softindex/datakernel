import React, {useEffect} from 'react';
import path from 'path';
import {Redirect, withRouter} from 'react-router-dom';

function AfterAuthRedirect({history}) {
  const redirectURI = localStorage.getItem('redirectURI') || '/';
  useEffect(() => {
    if (redirectURI !== '/') {
      history.push(path.join(redirectURI));
    }
    return () => {
      localStorage.clear();
    }
  }, [redirectURI]);

  return <Redirect to='/'/>;
}

export default withRouter(AfterAuthRedirect);
