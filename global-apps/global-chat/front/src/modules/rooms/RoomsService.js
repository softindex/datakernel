import Service from '../../common/Service';

const RETRY_CHECKOUT_TIMEOUT = 1000;

class RoomsService extends Service {
  constructor(roomsOTStateManager, messagingURL, contactsService) {
    super({
      rooms: [],
      ready: false,
    });
    this._roomsOTStateManager = roomsOTStateManager;
    this._reconnectTimeout = null;
    this._messagingURL = messagingURL;
    this._contactsService = contactsService;
  }

  async init() {
    // Get initial state
    try {
      await this._roomsOTStateManager.checkout();
    } catch (err) {
      console.error(err);
      await this._reconnectDelay();
      await this.init();
      return;
    }

    this._onStateChange();

    this._roomsOTStateManager.addChangeListener(this._onStateChange);
    this._contactsService.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._roomsOTStateManager.removeChangeListener(this._onStateChange);
    this._contactsService.removeChangeListener(this._onStateChange);
  }

  createRoom(name, participants) {
    return fetch(this._messagingURL + '/add', {
      method: 'POST',
      body: JSON.stringify([...participants])
    })
      .then(response => {
        if (response.status >= 400 && response.status < 600) {
          throw new Error("Bad response from server");
        }
      });
  }

  quitRoom(id) {
    return fetch(this._messagingURL + '/delete', {
      method: 'POST',
      body: JSON.stringify(id)
    });
  }

  _onStateChange = () => {
    this.setState({
      rooms: this._getRooms(),
      ready: true
    });
  };

  _getRooms() {
    const otState = [...this._roomsOTStateManager.getState()].map(key => JSON.parse(key));
    const contactState = [...this._contactsService.getAll().contacts].map(([contactPublicKey, contact]) => ({
      id: null,
      name: contact.name,
      participants: [contactPublicKey]
    }));
    return [...otState, ...contactState];
  }

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_CHECKOUT_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._roomsOTStateManager.sync();
    } catch (err) {
      console.error(err);
      await this._sync();
    }
  }
}

export default RoomsService;
