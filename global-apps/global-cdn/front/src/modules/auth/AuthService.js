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

  authByToken(token) {
    this.setStore({loading: true});
    return fetch(`/auth?token=${token}`)
      .then(response => {
        if (response.status !== 200) {
          throw new Error("Authorization failed: " + response.statusText);
        }
        return response.text();
      })
      .then(text => {
        localStorage.setItem('publicKey', text);
        this.setStore({authorized: true, publicKey: text, error: null});
      })
      .catch(error => this.setStore({error}))
      .finally(() => this.setStore({loading: false}));
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
