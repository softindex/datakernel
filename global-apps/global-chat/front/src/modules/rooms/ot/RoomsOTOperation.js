import {isArraysEqual} from "../../../common/utils";

class RoomsOTOperation {
  constructor(roomId, participants, remove) {
    this._roomId = roomId;
    this._participants = participants;
    this._removed = remove;
  }

  static EMPTY = new RoomsOTOperation(null, [], false);

  static createFromJson(json) {
    const room = json['shared repo'];
    return new RoomsOTOperation(room.id, room.participants, json.remove);
  }

  apply(state) {
    if (this._removed) {
      state.delete(this._roomId);
    } else {
      state.set(this._roomId, {
        participants: this._participants
      });
    }

    return state;
  }

  isEmpty() {
    return this._participants.length === 0;
  }

  invert() {
    return new RoomsOTOperation(this._roomId, this._participants, !this._removed);
  }

  isEqual(roomOTOperation) {
    return (
      roomOTOperation._roomId === this._roomId
      && isArraysEqual(roomOTOperation._participants, this._participants)
      && roomOTOperation._removed === this._removed
    );
  }

  toJSON() {
    return {
      'shared repo': {
        id: this._roomId,
        participants: this._participants
      },
      remove: this._removed
    };
  }
}

export default RoomsOTOperation;
