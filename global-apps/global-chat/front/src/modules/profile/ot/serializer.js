import ProfileOTOperation from './ProfileOTOperation';

const profileSerializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    return ProfileOTOperation.createFromJson(value);
  }
};

export default profileSerializer;
