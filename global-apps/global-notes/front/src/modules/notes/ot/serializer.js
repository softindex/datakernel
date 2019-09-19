import NotesOTOperation from './NotesOTOperation';

const profileSerializer = {
  serialize(value) {
    return value.toJSON();
  },

  deserialize(value) {
    return NotesOTOperation.createFromJson(value);
  }
};

export default profileSerializer;
