import {Redirect} from 'react-router-dom';
import AuthContext from '../modules/auth/AuthContext';
import connectService from '../common/connectService';

function Authorization({authorized}) {
  if (authorized) {
    return;
  <
    Redirect;
    to = '/folders' / >
  }
  return;
<
  Redirect;
  to = '/sign-up' / >
}

export default connectService(AuthContext, ({authorized}) => ({authorized}))(Authorization);
