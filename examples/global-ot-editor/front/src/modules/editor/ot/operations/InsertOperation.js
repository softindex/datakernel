import DeleteOperation from './DeleteOperation';

class InsertOperation {
  constructor(position, content) {
    this.position = position;
    this.content = content;
  }

  apply(state) {
    return (
      state.substr(0, this.position) +
      this.content +
      state.substr(this.position)
    );
  }

  invert() {
    return new DeleteOperation(this.position, this.content);
  }

  isEqual(insertOperation) {
    return insertOperation.position === this.position && insertOperation.content === this.content;
  }

  isEmpty() {
    return this.content.length === 0;
  }
}

export default InsertOperation;
