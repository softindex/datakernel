import * as types from '../MESSAGE_TYPES';
import MessageOperation from './MessageOperation';
import CallOperation from './CallOperation';
import DropCallOperation from './DropCallOperation';
import HandleCallOperation from './HandleCallOperation';

const serializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    switch (value.type) {
      case types.MESSAGE:
        return MessageOperation.createFromJson(value.value);
      case types.CALL:
        return CallOperation.createFromJson(value.value);
      case types.DROP:
        return DropCallOperation.createFromJson(value.value);
      case types.HANDLE:
        return HandleCallOperation.createFromJson(value.value);
      default:
        throw new Error('Unprocessable type of value');
    }
  }
};

export default serializer;
