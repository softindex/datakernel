import RoomsOTOperation from './RoomsOTOperation';
import Room from "./Room";

const roomsSerializer = {
  serialize(value) {
    return {
      'shared repo': value.room,
      remove: value.remove
    };
  },

  deserialize(value) {
    let room = value['shared repo'];
    return new RoomsOTOperation(new Room(room.id, room.participants), value.remove);
  }
};

export default roomsSerializer;
