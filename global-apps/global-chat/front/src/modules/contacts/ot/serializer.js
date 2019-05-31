import ContactsOTOperation from './ContactsOTOperation';

const contactsSerializer = {
  serialize(value) {
    return {
      pubKey: value.pubKey,
      name: value.name,
      remove: value.remove
    };
  },

  deserialize(value) {
    return new ContactsOTOperation(value.pubKey, value.name, value.remove);
  }
};

export default contactsSerializer;
