import RoomsOTOperation from "./ot/RoomsOTOperation";
import {delay, randomString, Service} from 'global-apps-common';
import {createDialogRoomId, RETRY_TIMEOUT} from '../../common/utils';

const ROOM_ID_LENGTH = 32;

class RoomsService extends Service {
  constructor(roomsOTStateManager, pubicKey) {
    super({
      rooms: new Map(),
      roomsReady: false
    });
    this._roomsOTStateManager = roomsOTStateManager;
    this._reconnectDelay = null;
    this._resyncDelay = null;
    this._myPublicKey = pubicKey;
  }

  static createFrom(roomsOTStateManager, pubKey) {
    return new RoomsService(roomsOTStateManager, pubKey);
  }

  async init() {
    try {
      await this._roomsOTStateManager.checkout();
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

    this._onStateChange();

    this._roomsOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
    this._roomsOTStateManager.removeChangeListener(this._onStateChange);
  }

  /**
   * @param {Iterable<string>} participants
   * @returns {Promise<string>}
   */
  async createRoom(participants) {
    const roomId = randomString(ROOM_ID_LENGTH);
    await this._createRoom(roomId, [...participants, this._myPublicKey]);
    return roomId;
  }

  async createDialog(participantPublicKey) {
    let participants = [this._myPublicKey];
    if (this._myPublicKey !== participantPublicKey) {
      participants.push(participantPublicKey);
    }

    const roomId = createDialogRoomId(this._myPublicKey, participantPublicKey);
    const roomExists = [...this.state.rooms].find(([id]) => id === roomId);
    if (!roomExists) {
      await this._createRoom(roomId, participants);
    }

    return roomId;
  }

  async quitRoom(roomId) {
    const room = this.state.rooms.get(roomId);
    if (!room) {
      return;
    }

    const deleteRoomOperation = new RoomsOTOperation({
      [roomId]: {
        name: "",
        participants: room.participants,
        remove: true
      }
    });
    this._roomsOTStateManager.add([deleteRoomOperation]);
    await this._sync();
  }

  async _createRoom(roomId, participants) {
    const addRoomOperation = new RoomsOTOperation({
      [roomId]: {
        name: "",
        participants,
        remove: false
      }
    });
    this._roomsOTStateManager.add([addRoomOperation]);
    await this._sync();
  }

  _onStateChange = () => {
    this.setState({
      rooms: this._getRooms(),
      roomsReady: true
    });
  };

  _getRooms() {
    const rooms = [...this._roomsOTStateManager.getState()]
      .map(([roomId, room]) => (
        [roomId, {
          participants: room.participants,
          dialog: room.participants.length === 2 && roomId === createDialogRoomId(this._myPublicKey,
            room.participants.find(publicKey => publicKey !== this._myPublicKey))
        }]
      ));
    return new Map(rooms);
  }

  async _sync() {
    try {
      await this._roomsOTStateManager.sync();
    } catch (err) {
      console.log(err);
      this._resyncDelay = delay(RETRY_TIMEOUT);
      try {
        await this._resyncDelay.promise;
      } catch (err) {
        return;
      }
      await this._sync();
    }
  }
}

export default RoomsService;
