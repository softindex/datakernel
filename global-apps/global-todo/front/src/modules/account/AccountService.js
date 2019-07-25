import Service from '../../common/Service';

class AccountService extends Service {
  constructor(appStoreUrl, cookies) {
    super({
      error: null,
      authorized: false,
      privateKey: null,
      loading: false
    });
    this._appStoreUrl = appStoreUrl;
    this._cookies = cookies;
  }

  init() {
    const privateKey = this._cookies.get('Key');
    if (privateKey) {
      this.setState({
        authorized: true,
        privateKey,
      });
    }
  }

  authByPrivateKey = privateKey => {
    this._cookies.set('Key', privateKey);
    this.setState({
      authorized: true,
      privateKey,
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

  authWithAppStore() {
    window.location.href = this._appStoreUrl + '/oauth?redirectURI=' + window.location.origin + '/sign-up/oauth';
  };

  logout() {
    this._cookies.remove('Key');
    this.setState({
      authorized: false,
      error: null,
      loading: false,
      privateKey: null,
    });
  }

}

export default AccountService;
