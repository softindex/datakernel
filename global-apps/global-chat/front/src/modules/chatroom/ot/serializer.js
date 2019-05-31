import ChatRoomOTOperation from './ChatRoomOTOperation';

const serializer = {
  serialize(value) {
    return {
      message: value.message,
      remove: value.remove
    };
  },

  deserialize(value) {
    return new ChatRoomOTOperation(value.message, value.remove);
  }
};

export default serializer;
