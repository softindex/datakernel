import ChatRoomOTOperation from './ChatRoomOTOperation';

const serializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    switch (value.type) {
      // TODO: case 'Call', case 'Drop', case 'Handle'
      case 'Message':
        return ChatRoomOTOperation.createFromJson(value);
      default:
        throw new Error('Unknown type');
    }
  }
};

export default serializer;
