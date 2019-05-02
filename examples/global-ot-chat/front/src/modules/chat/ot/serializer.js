import ChatOTOperation from './ChatOTOperation';

const serializer = {
  serialize(value) {
    return {
      message: value.message,
      remove: value.remove
    };
  },

  deserialize(value) {
    return new ChatOTOperation(value.message, value.remove);
  }
};

export default serializer;
