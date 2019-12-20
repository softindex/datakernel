import url from 'url';
import {request} from './request';
import marked from "marked";

export class GlobalAppStoreAPI {
  constructor(request, url = '') {
    this._request = request;
    this._url = url;
  }

  static create(url) {
    return new GlobalAppStoreAPI(request, url);
  }

  async login(email, password) {
    await this._request(url.resolve(this._url, '/api/auth/login'), {
      method: 'POST',
      body: JSON.stringify([email, password])
    });
  }

  async signup(newUser) {
    await this._request(url.resolve(this._url, '/api/users/register'), {
      method: 'POST',
      body: JSON.stringify({
        username: newUser.username !== "" ? newUser.username : null,
        password: newUser.password,
        email: newUser.email,
        firstName: newUser.firstName,
        lastName: newUser.lastName
      })
    });
  }

  async signupByGoogle(newUser) {
    await this._request(url.resolve(this._url, '/api/users/register/google'), {
      method: 'POST',
      body: JSON.stringify({
        username: newUser.username,
        email: newUser.email,
        firstName: newUser.firstName,
        lastName: newUser.lastName,
        accessToken: newUser.accessToken
      })
    });
  }

  async logout() {
    await this._request(url.resolve(this._url, '/api/auth/logout'), {
      method: 'POST'
    });
  }

  sendGoogleProfileCode(tokenCode) {
    return this._request(url.resolve(this._url, '/api/auth/googleTokenLogin'), {
      method: 'POST',
      body: "code=" + tokenCode,
      headers: { 'Content-type': 'application/x-www-form-urlencoded' }
    })
      .then(response => response && response.json());
  }

  getApplications() {
    return this._request(url.resolve(this._url, '/api/apps/list'))
      .then(response => response.json());
  }

  getRegistries() {
    return this._request(url.resolve(this._url, '/api/appRegistry/query'))
      .then(response => response.json());
  }

  getHosting() {
    return this._request(url.resolve(this._url, '/api/hostings/list'))
      .then(response => response.json())
      .then(hostingArray => hostingArray.map(hostingItem => ({
        ...hostingItem,
        terms: marked(hostingItem.terms.replace(/<.*?>/g, ''))
      })));
  }

  installApplication(hostingId, appId) {
    return this._request(url.resolve(this._url, '/api/appRegistry/requestInstall'), {
      method: 'POST',
      body: JSON.stringify([hostingId, appId])
    }).then(response => response.json());
  }

  deleteApplication(hostingId, appId) {
    return this._request(url.resolve(this._url, '/api/appRegistry/requestRemoval'), {
      method: 'POST',
      body: JSON.stringify([hostingId, appId])
    });
  }

  getHostingInfo(redirectUri) {
    return this._request(url.resolve(this._url, `/api/appRegistry/checkHost?host=${new URL(redirectUri).host}`))
      .then(response => response.json());
  }

  getKeys() {
    return this._request(url.resolve(this._url, '/api/users/keys/pairs'), {method: 'GET'})
      .then(response => response.json())
      .then(([firstPair]) => firstPair);
  }

  getPublicKey() {
    return this.getKeys()
      .then((keys) => keys[1]);
  }

  getPrivateKey() {
    return this.getKeys()
      .then((keys) => keys[0]);
  }

  updateProfile(profileChanges) {
    return this._request(url.resolve(this._url, 'api/users/profile'), {
      method: 'POST',
      body: JSON.stringify({
        username: profileChanges.username || (profileChanges.username === ''? '' : null),
        firstName: profileChanges.firstName || (profileChanges.firstName === ''? '' : null),
        lastName: profileChanges.lastName || (profileChanges.lastName === ''? '' : null),
        email: profileChanges.email || (profileChanges.email === ''? '' : null),
        newPassword: profileChanges.newPassword || null,
        oldPassword: profileChanges.oldPassword || null,
        isPublic: typeof profileChanges.isPublic === 'boolean' ? profileChanges.isPublic : null
      })
    });
  }

  getMyAppStoreProfile() {
    return this._request(url.resolve(this._url, 'api/users/profile'))
      .then(response => response.json());
  }

  search(searchField) {
    return this._request(url.resolve(this._url, `/api/users/lookup?query=${encodeURIComponent(searchField)}`))
      .then(response => response.json())
  }

  getUserByPublicKey(publicKey) {
    return this._request(url.resolve(this._url, `/api/users/${publicKey}`))
      .then(response => response.json())
      .catch(error => {
        if (error.statusCode === 404) {
          return null;
        }

        throw error;
      });
  }

  getProfile() {
    return this._request(url.resolve(this._url, 'api/users/myProfile'))
      .then(response => response.json());
  }
}
