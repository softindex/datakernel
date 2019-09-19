class RenameDocument {
  constructor(id, prev, next, timestamp) {
    this.id = id;
    this.prev = prev;
    this.next = next;
    this.timestamp = timestamp;
  }

  static createFromJson(json) {
    const {changeName} = json;
    return new RenameDocument(json.id, changeName.prev, changeName.next, changeName.timestamp);
  }

  apply(state) {
    const prevDocument = state.get(this.id);
    if (prevDocument) {
      state.set(this.id, {
        participants: prevDocument.participants,
        name: this.next
      });
    }
    return state;
  }

  isEmpty() {
    return this.prev === this.next;
  }

  invert() {
    return new RenameDocument(this.id, this.next, this.prev, this.timestamp);
  }

  isEqual(renameDocument) {
    return (
      renameDocument.id === this.id
      && renameDocument.prev === this.prev
      && renameDocument.next === this.next
      && renameDocument.timestamp === this.timestamp
    );
  }

  toJSON() {
    return {
      id: this.id,
      changeName: {
        prev: this.prev,
        next: this.next,
        timestamp: this.timestamp
      }
    };
  }
}

export default RenameDocument;
