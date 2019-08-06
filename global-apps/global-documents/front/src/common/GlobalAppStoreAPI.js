import url from 'url';
import request from './request';

class GlobalAppStoreAPI {
  constructor(request, url = '') {
    this._request = request;
    this._url = url;
  }

  static create(url) {
    return new GlobalAppStoreAPI(request, url);
  }

  search(searchField) {
    return this._request(url.resolve(this._url, `/api/users/lookup?query=${encodeURIComponent(searchField)}`))
      .then(response => response.json())
  }

  getUserByPublicKey(publicKey) {
    return this._request(url.resolve(this._url, `/api/users/${publicKey}`))
      .then(response => response.json())
  }

  getProfile() {
    return this._request(url.resolve(this._url, 'api/users/profile'))
      .then(response => response.json());
  }
}

export default GlobalAppStoreAPI;