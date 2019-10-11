import MapOTOperation from './MapOTOperation';

const mapOperationSerializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    return MapOTOperation.createFromJson(value);
  }
};

export default mapOperationSerializer;
