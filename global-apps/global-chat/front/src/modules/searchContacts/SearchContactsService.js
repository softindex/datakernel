import {retry, Service, delay, GlobalAppStoreAPI} from 'global-apps-common';
import {RETRY_TIMEOUT} from '../../common/utils';

class SearchContactsService extends Service {
  constructor(contactsOTStateManager, publicKey, globalAppStoreAPI) {
    super({
      searchContacts: new Map(),
      searchReady: false,
      search: ''
    });

    this._myPublicKey = publicKey;
    this._globalAppStoreAPI = globalAppStoreAPI;
    this._contactsOTStateManager = contactsOTStateManager;
    this._contactsCheckoutPromise = null;
    this._reconnectDelay = null;
    this._resyncDelay = null;
  }

  static createFrom(contactsOTStateManager, publicKey) {
    return new SearchContactsService(
      contactsOTStateManager,
      publicKey,
      GlobalAppStoreAPI.create(process.env.REACT_APP_GLOBAL_OAUTH_LINK)
    );
  }

  async init() {
    this._contactsCheckoutPromise = retry(() => this._contactsOTStateManager.checkout(), RETRY_TIMEOUT);
    try {
      await this._contactsCheckoutPromise;
    } catch (err) {
      console.log(err);

      this._reconnectDelay = delay(RETRY_TIMEOUT);
      try {
        await this._reconnectDelay.promise;
      } catch (err) {
        return;
      }

      await this.init();
      return;
    }

    this.search(this.state.search);
    this._contactsOTStateManager.addChangeListener(() => {
      this.search(this.state.search);
    });
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
    this._contactsCheckoutPromise.stop();
    this._contactsOTStateManager.removeChangeListener(() => this.search(this.state.search));
  }

  search(searchField) {
    this.setState({search: searchField, searchReady: false});
    const contacts = this._contactsOTStateManager.getState();

    this._globalAppStoreAPI.search(this.state.search)
      .then(appStoreContacts => {
        const searchContacts = new Map(
          [...appStoreContacts]
            .map(({profile, pubKey}) => ([pubKey, profile]))
            .filter(([publicKey,]) => !contacts.has(publicKey) && publicKey !== this._myPublicKey)
        );
        this.setState({
          searchContacts,
          searchReady: true
        });
      })
      .catch((error) => console.error(error));
  }
}

export default SearchContactsService;
