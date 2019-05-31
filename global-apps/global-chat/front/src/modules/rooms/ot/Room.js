import {arraysEq} from "../../../common/utils";

class Room {
  constructor(id, participants) {
    this.id = id;
    this.participants = participants;
  }

  isEmpty() {
    return this.participants.length === 0;
  }

  isEqual(room) {
    return (
      room.id === this.id &&
      arraysEq(room.participants, this.participants)
    );
  }

}

export default Room;


