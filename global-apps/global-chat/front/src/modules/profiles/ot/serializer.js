import ProfilesOTOperation from './ProfilesOTOperation';

const profilesSerializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    return ProfilesOTOperation.createFromJson(value);
  }
};

export default profilesSerializer;
