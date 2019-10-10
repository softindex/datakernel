import mapValues from 'lodash/mapValues';
import values from 'lodash/values';

class RoomsOTOperation {
  constructor(rooms) {
    this._rooms = rooms;
  }

  static createFromJson(json) {
    return new RoomsOTOperation(json);
  }

  apply(state) {
    Object.entries(this._rooms).forEach(([id, room]) => {
      if (room.remove) {
        state.delete(id);
      } else {
        state.set(id, {
          name: room.name,
          participants: room.participants
        })
      }
    });

    return state;
  }

  get rooms() {
    return this._rooms;
  }

  isEmpty() {
    return values(this._rooms).every(el => el.participants.length === 0);
  }

  invert() {
    return new RoomsOTOperation(mapValues(this._rooms, room => ({
      name: room.name,
      participants: room.participants,
      remove: !room.remove
    })));
  }

  toJSON() {
    return {
      type: 'CreateOrDropRepo',
      value: this._rooms
    };
  }
}

export default RoomsOTOperation;
