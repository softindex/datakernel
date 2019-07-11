import Service from '../../common/Service';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import roomsOTSystem from "./ot/RoomsOTSystem";
import roomsSerializer from "./ot/serializer";
import RoomsOTOperation from "./ot/RoomsOTOperation";
import {randomString, wait, toEmoji, createDialogRoomId} from '../../common/utils';

const RETRY_TIMEOUT = 1000;
const ROOM_ID_LENGTH = 32;

class RoomsService extends Service {
  constructor(roomsOTStateManager, contactsService, pubicKey) {
    super({
      rooms: new Map(),
      ready: false,
    });
    this._roomsOTStateManager = roomsOTStateManager;
    this._reconnectTimeout = null;
    this._contactsService = contactsService;
    this._myPublicKey = pubicKey;
    this._getRoomName.bind(this);
  }

  static createForm(contactsService, pubKey) {
    const roomsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/rooms',
      serializer: roomsSerializer
    });
    const roomsOTStateManager = new OTStateManager(() => new Map(), roomsOTNode, roomsOTSystem);
    return new RoomsService(roomsOTStateManager, contactsService, pubKey);
  }

  async init() {
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
    await this._createRoom(roomId, name, [...participants, this._myPublicKey]);
  }

  async createDialog(participantId) {
    const participants = [this._myPublicKey, participantId];
    const roomId = createDialogRoomId(...participants);
    const {name} = this._contactsService.state.contacts.get(participantId);

    const roomExists = [...this.state.rooms]
      .find(([id, {virtual}]) => {
        return id === roomId && !virtual;
      });

    if (!roomExists) {
      await this._createRoom(roomId, name, participants);
    }
  }

  async quitRoom(roomId) {
    const room = this.state.rooms.get(roomId);
    if (!room) {
      return ;
    }

    const deleteRoomOperation = new RoomsOTOperation(roomId, room.participants, true);
    this._roomsOTStateManager.add([deleteRoomOperation]);
    await this._sync();
  }

  async _createRoom(roomId, name, participants) {
    const addRoomOperation = new RoomsOTOperation(roomId, participants, false);
    this._roomsOTStateManager.add([addRoomOperation]);
    await this._sync();
  }

  _onStateChange = () => {
    this.setState({
      rooms: this._getRooms(),
      ready: true
    });
  };

  _getRoomName(room) {
    return room.participants
      .filter(participantPublicKey => participantPublicKey !== this._myPublicKey)
      .map(participantPublicKey => {
        return this._contactsService.getContactName(participantPublicKey) || toEmoji(participantPublicKey, 3);
      })
      .join(', ');
  }

  _getRooms() {
    const otState = [...this._roomsOTStateManager.getState()]
      .map(([roomId, room]) => {
        return {
          id: roomId,
          name: this._getRoomName(room),
          participants: room.participants,
          virtual: false
        }
      });
    const contactState = [...this._contactsService.getAll().contacts].map(([contactPublicKey, contact]) => {
      const participants = [this._myPublicKey, contactPublicKey];
      return {
        id: createDialogRoomId(...participants),
        name: contact.name,
        participants,
        virtual: true
      };
    });

    return new Map([
      ...contactState,
      ...otState
    ].map(({id, name, participants, virtual}) => ([id, {name, participants, virtual}])));
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
