import React from 'react';
import qs from 'query-string';
import connectService from '../../common/connectService';
import {Redirect} from 'react-router-dom';
import AccountContext from "../../modules/account/AccountContext";

class AccountCallback extends React.Component {
  componentDidMount() {
    const params = qs.parse(this.props.location.search);

    if (params.privateKey) {
      this.props.authByPrivateKey(params.privateKey);
    }
  }

  render() {
    return <Redirect to='/'/>;
  }
}

export default connectService(
  AccountContext, (state, accountService) => ({
    authByPrivateKey(privateKey) {
      accountService.authByPrivateKey(privateKey);
    }
  })
)(
  AccountCallback
);