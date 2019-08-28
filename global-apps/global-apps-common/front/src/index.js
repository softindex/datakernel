import React from 'react';
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
import Avatar from './avatar/Avatar';
import useService from './useService';
import * as utils from './utils/utils';

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
  Avatar
}

// export * from './auth/AuthService';
// export * from './auth/AuthContext';
// export * from './auth/checkAuth';
// export * from './globalAppStoreAPI/GlobalAppStoreAPI';
// export * from './globalAppStoreAPI/request';
// export * from './auth/OAuthCallback/OAuthCallback';
// export * from './connectService/connectService';
// export * from './connectService/Service';
// export * from './signUp/SignUp/SignUp';
// export * from './signUp/SignUpAbstractionImage/SignUpAbstractionImage';
// export * from './avatar/Avatar';
export * from './utils/utils';
export * from './useService';
export * from './DI';
