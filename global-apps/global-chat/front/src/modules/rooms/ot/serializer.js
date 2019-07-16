import RoomsOTOperation from './RoomsOTOperation';

const roomsSerializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    return RoomsOTOperation.createFromJson(value);
  }
};

export default roomsSerializer;
