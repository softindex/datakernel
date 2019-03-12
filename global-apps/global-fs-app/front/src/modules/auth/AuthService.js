import Store from '../../common/Store';

class AuthService extends Store {
  constructor(keyPairGenerator, cookies, localStorage) {
    super({
      error: null,
      authorized: false,
      privateKey: null,
      publicKey: null,
      loading: false
    });
    this._keyPairGenerator = keyPairGenerator;
    this._cookies = cookies;
    this._localStorage = localStorage;
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

  async authWithNewKey() {
    this.setStore({
      error: null,
      loading: true
    });

    let response;
    try {
      response = this._keyPairGenerator.generate();
    } catch (e) {
      this.setStore({
        error: e,
        loading: false
      });
      throw e;
    }

    this._cookies.set('Key', response.privateKey);
    this._localStorage.setItem('publicKey', response.publicKey);

    this.setStore({
      authorized: true,
      privateKey: response.privateKey,
      publicKey: response.publicKey,
      loading: false
    });
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
