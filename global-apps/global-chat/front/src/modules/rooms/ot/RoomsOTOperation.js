import Room from "./Room";

class RoomsOTOperation {
  constructor(room, remove) {
    this.room = room;
    this.remove = remove;
  }

  static EMPTY = new RoomsOTOperation(new Room('', []), false);

  apply(state) {
    const key = JSON.stringify(this.room);

    if (this.remove) {
      state.delete(key);
    } else {
      state.add(key);
    }

    return state;
  }

  isEmpty() {
    return this.room.isEmpty();
  }

  invert() {
    return new RoomsOTOperation(this.room, !this.remove);
  }

  isEqual(chatOTOperation) {
    return chatOTOperation.room.isEqual(this.room) && chatOTOperation.remove === this.remove;
  }
}

export default RoomsOTOperation;
