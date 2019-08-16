import MyProfileOTOperation from './MyProfileOTOperation';

const myProfileSerializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    return MyProfileOTOperation.createFromJson(value);
  }
};

export default myProfileSerializer;
