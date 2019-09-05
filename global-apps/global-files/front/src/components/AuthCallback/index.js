import React from 'react';
import qs from 'query-string';
import connectService from '../../common/connectService';
import AuthContext from '../../modules/auth/AuthContext';
import EC from 'elliptic';
import {Redirect} from 'react-router-dom';

const curve = new EC.ec('secp256k1');

class AuthCallback extends React.Component {
  render() {
    let params = qs.parse(this.props.location.search);
    let privateKey = params.privateKey;

    if (privateKey) {
      let keys = curve.keyFromPrivate(privateKey, 'hex');
      const publicKey = `${keys.getPublic().getX().toString('hex')}:${keys.getPublic().getY().toString('hex')}`;

      this.props.authService.authByKeys(publicKey, privateKey);
    }

    return <Redirect to='/'/>;
  }

}

export default connectService(AuthContext, (state, authService) => ({authService}))(AuthCallback);

