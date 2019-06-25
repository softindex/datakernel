import Store from '../../common/Store';

class AuthService extends Store {
  constructor(appStoreUrl, cookies, localStorage) {
    super({
      error: null,
      authorized: false,
      privateKey: null,
      publicKey: null,
      loading: false
    });
    this._cookies = cookies;
    this._localStorage = localStorage;
    this._appStoreUrl = appStoreUrl;
  }

  init() {
    const privateKey = this._cookies.get('Key');
    const publicKey = this._localStorage.getItem('publicKey');
    if (privateKey && publicKey) {
      this.setStore({
        authorized: true,
        privateKey,
        publicKey
      });
    }
  }

  authByFile(file) {
    return new Promise((resolve) => {
      const fileReader = new FileReader();
      fileReader.readAsText(file);
      fileReader.onload = () => {
        const [publicKey, privateKey] = fileReader.result.split('-');
        this.authByKeys(publicKey, privateKey);
        resolve();
      };
    });
  }

  authByKeys(publicKey, privateKey) {
    this._localStorage.setItem('publicKey', publicKey);
    this._cookies.set('Key', privateKey);

    this.setStore({
      authorized: true,
      privateKey,
      publicKey
    });
  }

  authWithAppStore() {
    window.location.href = this._appStoreUrl + '/extAuth?redirectUri=' + window.location.href + '/auth';
  }

  logout() {
    this._cookies.remove('Key');
    this._localStorage.removeItem('publicKey');

    this.setStore({
      authorized: false,
      privateKey: null,
      publicKey: null
    });
  }

  createKeysFile() {
    return new File([this.store.publicKey + '-' + this.store.privateKey], 'keys.dat', {
      type: 'text/plain;charset=utf-8'
    });
  }
}

export default AuthService;
