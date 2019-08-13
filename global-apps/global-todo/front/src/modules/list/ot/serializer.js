import MapOTOperation from './MapOTOperation';

const profileSerializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    return MapOTOperation.createFromJson(value);
  }
};

export default profileSerializer;
