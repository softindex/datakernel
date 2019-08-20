import React, {Component} from 'react';
import PropTypes from 'prop-types';
import styles from './styles.css'
import checkAuth from './auth/checkAuth';
import request from './globalAppStoreAPI/request';
import connectService from './connectService/connectService';
import Service from './connectService/Service';
import AuthContext from './auth/AuthContext';
import AuthService from './auth/AuthService';
import GlobalAppStoreAPI from './globalAppStoreAPI/GlobalAppStoreAPI';
import SignUpAbstractionImage from './signUp/SignUpAbstractionImage/SignUpAbstractionImage';
import SignUp from './signUp/SignUp/SignUp';
import OAuthCallback from './auth/OAuthCallback/OAuthCallback';
import {
  getAppStoreContactName,
  getAvatarLetters,
  getRoomName,
  createDialogRoomId,
  randomString,
  retry,
  ROOT_COMMIT_ID,
  toEmoji,
  wait
} from './utils/utils';

export default class ExampleComponent extends Component {
  static propTypes = {
    text: PropTypes.string
  };

  render() {
    const {text} = this.props;

    return (
      <div className={styles.test}>
        Example library Component: {text}
      </div>
    )
  }
}

export {
  checkAuth,
  request,
  connectService,
  SignUp,
  Service,
  AuthContext,
  AuthService,
  SignUpAbstractionImage,
  GlobalAppStoreAPI,
  OAuthCallback,
  getAppStoreContactName,
  getAvatarLetters,
  getRoomName,
  createDialogRoomId,
  randomString,
  retry,
  ROOT_COMMIT_ID,
  toEmoji,
  wait
}




