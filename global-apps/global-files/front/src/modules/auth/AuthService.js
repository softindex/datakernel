import Store from '../../common/Store';

class AuthService extends Store {
  constructor(appStoreUrl, cookies, sessionId) {
    super({
      error: null,
      authorized: false,
      publicKey: null,
      loading: false,
      wasAuthorized: false
    });
    this._appStoreUrl = appStoreUrl;
    this._cookies = cookies;
    this._sessionId = sessionId;
  }

  init() {
    const sessionString = this._cookies.get(this._sessionId);
    if (sessionString) {
      const publicKey = localStorage.getItem('publicKey');
      if (publicKey) {
        this.setStore({authorized: true, publicKey});
      } else {
        this._cookies.remove(this._sessionId);
      }
    } else {
      localStorage.removeItem('publicKey');
    }
  }

  authByAppStore() {
    window.location.href = this._appStoreUrl + '/oauth?redirectURI=' + window.location.href + '/auth';
  }

  authByFile(file) {
    return new Promise((resolve, reject) => {
      const fileReader = new FileReader();
      fileReader.readAsText(file);
      fileReader.onload = () => {
        const privateKey = fileReader.result.replace(/\r?\n|\r/, "");
        this._doAuth(fetch('/authByKey', {method: 'post', body: privateKey}))
          .then(resolve)
          .catch(reject);
        resolve();
      };
      fileReader.onerror = error => {
        this.setStore({error});
        reject(error);
      };
    });
  };

  authByToken(token) {
    return this._doAuth(fetch(`/auth?token=${token}`));
  }

  _doAuth(fetchPromise) {
    this.setStore({loading: true});
    return fetchPromise
      .then(response => {
        this.setStore({loading: false});
        if (response.status !== 200) {
          this.setStore({error: new Error("Authorization failed: " + response.statusText)});
        } else {
          response.text()
            .then(text => {
              localStorage.setItem('publicKey', text);
              this.setStore({authorized: true, publicKey: text, error: null});
            })
            .catch(error => this.setStore({error}));
        }
      });
  }

  async logout() {
    try {
      await fetch('/logout', {method: 'POST'});
    } catch (e) {
      console.warn('Log out call to server failed', e);
    }

    this._cookies.remove(this._sessionId);
    localStorage.removeItem('publicKey');

    this.setStore({
      authorized: false,
      error: null,
      loading: false,
      publicKey: null,
      wasAuthorized: true
    });
  }
}

export default AuthService;
