import ChatOTOperation from './ChatOTOperation';

const serializer = {
  serialize(value) {
    return {
      timestamp: value.timestamp,
      author: value.author,
      content: value.content,
      isDelete: value.isDeleted
    };
  },

  deserialize(value) {
    return new ChatOTOperation(value.timestamp, value.author, value.content, value.isDelete);
  }
};

export default serializer;
