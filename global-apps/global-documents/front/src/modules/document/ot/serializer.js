import DeleteOperation from './DeleteOperation';
import InsertOperation from './InsertOperation';

const serializer = {
  serialize(value) {
    return {
      type: value instanceof DeleteOperation ? 'Delete' : 'Insert',
      value: {
        pos: value.position,
        content: value.content
      }
    };
  },

  deserialize(value) {
    switch (value.type) {
      case 'Insert':
        return new InsertOperation(value.value.pos, value.value.content);
      case 'Delete':
        return new DeleteOperation(value.value.pos, value.value.content);
      default:
        return value;
    }
  }
};

export default serializer;
