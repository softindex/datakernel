import crypto from 'crypto';
import Service from '../../common/Service';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import roomsOTSystem from "./ot/RoomsOTSystem";
import roomsSerializer from "./ot/serializer";
import RoomsOTOperation from "./ot/RoomsOTOperation";
import {randomString, wait} from '../../common/utils';

const RETRY_TIMEOUT = 1000;
const ROOM_ID_LENGTH = 32;

class RoomsService extends Service {
  constructor(roomsOTStateManager, contactsService) {
    super({
      rooms: [],
      ready: false,
    });
    this._roomsOTStateManager = roomsOTStateManager;
    this._reconnectTimeout = null;
    this._contactsService = contactsService;
    this._roomNames = new Map();
  }

  static createForm(contactsService) {
    const roomsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/rooms',
      serializer: roomsSerializer
    });
    const roomsOTStateManager = new OTStateManager(() => new Map(), roomsOTNode, roomsOTSystem);
    return new RoomsService(roomsOTStateManager, contactsService);
  }

  async init() {
    const roomNamesJson = localStorage.getItem('roomNames');
    if (roomNamesJson) {
      this._roomNames = new Map(JSON.parse(roomNamesJson));
    }

    // Get initial state
    try {
      await this._roomsOTStateManager.checkout();
    } catch (err) {
      console.log(err);
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

  async createRoom(name, participants) {
    const roomId = randomString(ROOM_ID_LENGTH);
    await this._createRoom(roomId, name, participants);
  }

  async createDialog(name, participants) { // TODO without name, participant
    const roomId = crypto.createHash('sha256').update(participants.join(';')).digest('hex');
    await this._createRoom(roomId, name, participants);
  }

  quitRoom(id) {
    return fetch(this._messagingURL + '/delete', {
      method: 'POST',
      body: JSON.stringify(id)
    });
  }

  async _createRoom(roomId, name, participants) {
    const addRoomOperation = new RoomsOTOperation(roomId, participants, false);

    this._roomNames.set(roomId, name);
    localStorage.setItem('roomNames', JSON.stringify([...this._roomNames]));

    this._roomsOTStateManager.add([addRoomOperation]);
    await this._sync();
  }

  _onStateChange = () => {
    this.setState({
      rooms: this._getRooms(),
      ready: true
    });
  };

  _getRooms() {
    const otState = [...this._roomsOTStateManager.getState()]
      .map(([roomId, roomData]) => ({
        id: roomId,
        ...roomData,
        name: this._roomNames.get(roomData.id)
      }));
    const contactState = [...this._contactsService.getAll().contacts].map(([contactPublicKey, contact]) => ({
      id: null,
      name: contact.name,
      participants: [contactPublicKey]
    }));
    return [
      ...otState
    ];
  }

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._roomsOTStateManager.sync();
    } catch (err) {
      console.log(err);
      await wait(RETRY_TIMEOUT);
      await this._sync();
    }
  }
}

export default RoomsService;
