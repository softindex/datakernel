import Service from '../../common/Service';

const RETRY_CHECKOUT_TIMEOUT = 1000;

class RoomsService extends Service {
  constructor(roomsOTStateManager, messagingURL) {
    super({
      rooms: [],
      ready: false,
    });
    this._roomsOTStateManager = roomsOTStateManager;
    this._reconnectTimeout = null;
    this._messagingURL = messagingURL;
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
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._roomsOTStateManager.removeChangeListener(this._onStateChange);
  }

  createRoom(participants) {
    return fetch(this._messagingURL + '/add', {
      method: 'POST',
      body: JSON.stringify(participants)
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
      rooms: this._getRoomsFromStateManager(),
      ready: true
    });
  };

  _getRoomsFromStateManager() {
    const otState = this._roomsOTStateManager.getState();
    return [...otState]
      .sort()
      .map(key => JSON.parse(key));
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
