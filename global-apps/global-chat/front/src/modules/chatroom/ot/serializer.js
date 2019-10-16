import ChatRoomOTOperation from './ChatRoomOTOperation';

const chatRoomSerializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    if (value.type === 'Message') {
      return ChatRoomOTOperation.createFromJson(value);
    }

    throw new Error('Unknown type');
  }
};

export default chatRoomSerializer;
