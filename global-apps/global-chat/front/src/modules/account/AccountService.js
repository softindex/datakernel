import Service from '../../common/Service';

let EC = require('elliptic').ec;

class AccountService extends Service {
  constructor(cookies) {
    super({
      authorized: false,
      privateKey: null,
      publicKey: null
    });
    this._cookies = cookies;
  }

  init() {
    const privateKey = this._cookies.get('Key');
    if (privateKey) {
      this.setState({
        authorized: true,
        privateKey,
        publicKey: this._getPublicKey(privateKey)
      });
    }
  }

  authByKey(privateKey) {
    this._cookies.set('Key', privateKey);
    const publicKey = this._getPublicKey(privateKey);
    this.setState({
      authorized: true,
      privateKey,
      publicKey
    });
  }

  _getPublicKey = (privateKey) => {
    const curve = new EC('secp256k1');
    let keys = curve.keyFromPrivate(privateKey, 'hex');
    return `${keys.getPublic().getX().toString('hex')}:${keys.getPublic().getY().toString('hex')}`;
  };

  logout() {
    this._cookies.remove('Key');
    this.setState({
      authorized: false,
      privateKey: null,
      publicKey: null
    });
  }
}

export default AccountService;
