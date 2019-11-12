import RoomsOTOperation from './RoomsOTOperation';

const roomsSerializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
   switch (value.type) {
      // TODO case 'RenameRepo':
      case 'CreateOrDropRepo':
        return RoomsOTOperation.createFromJson(value);
     default:
       throw new Error('Unknown type');
    }
  }
};

export default roomsSerializer;
