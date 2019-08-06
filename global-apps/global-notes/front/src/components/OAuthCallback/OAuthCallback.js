import React, {useEffect} from 'react';
import {Redirect} from 'react-router-dom';
import qs from 'query-string';
import connectService from '../../common/connectService';
import AccountContext from '../../modules/account/AccountContext';

function OAuthCallback(props) {
  useEffect(() => {
    const params = qs.parse(props.location.search);

    if (params.privateKey) {
      props.authByPrivateKey(params.privateKey);
    }
  });

  return (
    <Redirect to='/'/>
  );
}

export default connectService(
  AccountContext,
  (state, accountService) => ({
    authByPrivateKey(privateKey) {
      accountService.authByPrivateKey(privateKey);
    }
  })
)(
  OAuthCallback
);
