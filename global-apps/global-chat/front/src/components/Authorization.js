import React from 'react';
import {Redirect} from 'react-router-dom';
import connectService from '../common/connectService';
import AccountContext from "../modules/account/AccountContext";

function Authorization({authorized}) {
  if (authorized) {
    return <Redirect to='/rooms'/>
  }
  return <Redirect to='/sign-up'/>
}

export default connectService(AccountContext, ({authorized}) => ({authorized}))(Authorization);
