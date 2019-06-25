import ChatRoomOTOperation from './ChatRoomOTOperation';

const serializer = {
  serialize(value) {
    return value;
  },

  deserialize(value) {
    return ChatRoomOTOperation.createFromJson(value);
  }
};

export default serializer;
