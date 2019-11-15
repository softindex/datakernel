import {Service} from '../service/Service';
import {ValueStorage} from './ValueStorage';

export class AuthService extends Service {
  constructor(
    appStoreURL,
    oAuthURL,
    sessionValueStorage,
    publicKeyValueStorage,
    goToURL,
    createFileReader,
    fetch
  ) {
    super({
      error: null,
      authorized: false,
      publicKey: null,
      loading: false,
      wasAuthorized: false
    });
    this._appStoreURL = appStoreURL;
    this._oAuthURL = oAuthURL;
    this._sessionValueStorage = sessionValueStorage;
    this._publicKeyValueStorage = publicKeyValueStorage;
    this._goToURL = goToURL;
    this._createFileReader = createFileReader;
    this._fetch = fetch;
  }

  static create({appStoreURL, sessionIdField = 'sid', publicKeyField = 'publicKey'}) {
    return new AuthService(
      appStoreURL,
      window.location.origin + '/sign-up/auth',
      ValueStorage.createCookie(sessionIdField),
      ValueStorage.createLocalStorage(publicKeyField),
      url => location.href = url,
      () => new FileReader(),
      fetch.bind(window)
    );
  }

  init() {
    const sessionString = this._sessionValueStorage.get();
    if (!sessionString) {
      this._publicKeyValueStorage.remove();
      return
    }

    const publicKey = this._publicKeyValueStorage.get();
    if (publicKey) {
      this.setState({
        authorized: true,
        publicKey
      });
    } else {
      this._sessionValueStorage.remove();
    }
  }

  authByAppStore() {
    this._goToURL(this._appStoreURL + '/oauth?redirectURI=' + encodeURIComponent(this._oAuthURL));
  }

  authByToken(token) {
    this.setState({loading: true});
    return fetch(`/auth?token=${token}`)
      .then(response => {
        if (response.status !== 200) {
          throw new Error("Authorization failed: " + response.statusText);
        }

        return response.text();
      })
      .then(publicKey => {
        this._publicKeyValueStorage.set(publicKey);
        this.setState({
          authorized: true,
          publicKey,
          error: null
        });
      })
      .catch(error => this.setState({error}))
      .finally(() => this.setState({loading: false}));
  }

  async logout() {
    try {
      await this._fetch('/logout', {method: 'POST'});
    } catch (error) {
      console.warn('Log out call to server failed', error);
    }

    this._sessionValueStorage.remove();
    this._publicKeyValueStorage.remove();

    this.setState({
      authorized: false,
      error: null,
      loading: false,
      publicKey: null,
      wasAuthorized: true
    });
  }
}

