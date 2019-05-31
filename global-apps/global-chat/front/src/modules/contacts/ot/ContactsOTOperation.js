class ContactsOTOperation {
  constructor(pubKey, name, remove) {
    this.pubKey = pubKey;
    this.name = name;
    this.remove = remove;
  }

  static EMPTY = new ContactsOTOperation(null, '', false);

  apply(state) {
    if (this.remove) {
      state.delete(this.pubKey);
    } else {
      state.set(this.pubKey, this.name);
    }

    return state;
  }

  isEmpty() {
    return this.pubKey === null;
  }

  invert() {
    return new ContactsOTOperation(this.pubKey, this.name, !this.remove);
  }

  isEqual(contactsOTOperation) {
    return contactsOTOperation.publicKey === this.pubKey && contactsOTOperation.name === this.name && contactsOTOperation.remove === this.remove;
  }
}

export default ContactsOTOperation;
