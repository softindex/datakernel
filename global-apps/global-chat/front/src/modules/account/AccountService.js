import Service from '../../common/Service';

let EC = require('elliptic').ec;

class AccountService extends Service {
  constructor(appStoreUrl, cookies) {
    super({
      error: null,
      authorized: false,
      privateKey: null,
      publicKey: null,
      loading: false
    });
    this._appStoreUrl = appStoreUrl;
    this._cookies = cookies;
  }

  init() {
    const privateKey = this._cookies.get('Key');
    if (privateKey) {
      const publicKey = this.getPublicKey(privateKey);
      this.setState({
        authorized: true,
        privateKey,
        publicKey
      });
    }
  }

  authByPrivateKey = privateKey => {
    this._cookies.set('Key', privateKey);
    const publicKey = this.getPublicKey(privateKey);
    this.setState({
      authorized: true,
      privateKey,
      publicKey
    });
  };

  authByFile = file => {
    return new Promise((resolve) => {
      const fileReader = new FileReader();
      fileReader.readAsText(file);
      fileReader.onload = () => {
        const privateKey = fileReader.result.charAt(0);
        this.authByPrivateKey(privateKey);
        resolve();
      };
    });
  };

  authWithAppStore = () => {
    window.location.href = this._appStoreUrl + '/extAuth?redirectUri=' + window.location.href + '/auth';
  };

  logout() {
    this._cookies.remove('Key');
    this.setState({
      authorized: false,
      error: null,
      loading: false,
      privateKey: null,
      publicKey: null
    });
  }

  getPublicKey = (privateKey) => {
    const curve = new EC('secp256k1');
    let keys = curve.keyFromPrivate(privateKey, 'hex');
    return `${keys.getPublic().getX().toString('hex')}:${keys.getPublic().getY().toString('hex')}`;
  };
}

export default AccountService;
