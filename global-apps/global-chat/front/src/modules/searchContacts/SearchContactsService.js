import {retry, Service} from 'global-apps-common';
import {GlobalAppStoreAPI} from 'global-apps-common';

const RETRY_TIMEOUT = 1000;

class SearchContactsService extends Service {
  constructor(contactsOTStateManager, globalAppStoreAPI) {
    super({
      searchContacts: new Map(),
      searchReady: false,
      search: '',
      searchError: ''
    });

    this._globalAppStoreAPI = globalAppStoreAPI;
    this._contactsOTStateManager = contactsOTStateManager;
    this._contactsCheckoutPromise = null;
  }

  static createFrom(contactsOTStateManager) {
    return new SearchContactsService(
      contactsOTStateManager,
      GlobalAppStoreAPI.create(process.env.REACT_APP_GLOBAL_OAUTH_LINK)
    );
  }

  async init() {
    this._contactsCheckoutPromise = retry(() => this._contactsOTStateManager.checkout(), RETRY_TIMEOUT);
    await Promise.resolve(this._contactsCheckoutPromise);
    this.search(this.state.search);
    this._contactsOTStateManager.addChangeListener(() => this.search(this.state.search));
  }

  stop() {
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
            .filter(([publicKey,]) => !contacts.has(publicKey))
        );
        this.setState({
          searchContacts,
          searchReady: true
        });
      }).catch((error) => {
      console.error(error);
      this.setState({searchError: error.toString()})
    });
  }
}

export default SearchContactsService;
