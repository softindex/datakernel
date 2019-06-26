import Service from '../../common/Service';

let EC = require('elliptic').ec;

class AccountService extends Service {
  constructor(appStoreUrl, cookies, localStorage) {
    super({
      error: null,
      authorized: false,
      privateKey: null,
      publicKey: null,
      loading: false
    });
    this._appStoreUrl = appStoreUrl;
    this._cookies = cookies;
    this._localStorage = localStorage;
  }

  init() {
    const privateKey = this._cookies.get('Key');
    const publicKey = this._cookies.get('Key');
    if (privateKey) {
      this.setState({
        authorized: true,
        privateKey,
        publicKey
      });
    }
  }

  authByKey(publicKey, privateKey) {
    privateKey = privateKey.charAt(0);
    this._cookies.set('Key', privateKey);
    this._cookies.set('PublicKey', publicKey);
    this.setState({
      authorized: true,
      privateKey,
      publicKey
    });
  }

  authByFile(file) {
    return new Promise((resolve) => {
      const fileReader = new FileReader();
      fileReader.readAsText(file);
      fileReader.onload = () => {
        const [publicKey, privateKey] = fileReader.result.split('-');
        this.authByKey(publicKey, privateKey);
        resolve();
      };
    });
  }

  authWithAppStore() {
    window.location.href = this._appStoreUrl + '/extAuth?redirectUri=' + window.location.href + '/auth';
  }

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
