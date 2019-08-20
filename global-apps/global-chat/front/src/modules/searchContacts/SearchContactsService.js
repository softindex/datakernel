import {Service} from 'global-apps-common';
import {GlobalAppStoreAPI} from 'global-apps-common';

class SearchContactsService extends Service {
  constructor(globalAppStoreAPI) {
    super({
      searchContacts: new Map(),
      searchReady: false,
      search: '',
      error: ''
    });

    this._globalAppStoreAPI = globalAppStoreAPI;
  }

  static create() {
    return new SearchContactsService(GlobalAppStoreAPI.create(process.env.REACT_APP_GLOBAL_OAUTH_LINK));
  }

  getAppStoreContactName(publicKey) {
    if (this.state.searchContacts.get(publicKey)){
      const contact = this.state.searchContacts.get(publicKey);
      if (contact.firstName !== '' && contact.lastName !== '') {
        return contact.firstName + ' ' + contact.lastName;
      }
      return contact.username;
    }
  }

  search(searchField) {
    this.setState({search: searchField, searchReady: false});

    this._globalAppStoreAPI.search(this.state.search)
      .then(contacts => {
        const searchContacts = new Map([...contacts]
          .map(({profile, pubKey}) => ([pubKey, profile])));
        this.setState({searchContacts, searchReady: true});
      }).catch((error) => {
        console.error(error);
        this.setState({error: error.toString()})
    });
  }
}

export default SearchContactsService;
