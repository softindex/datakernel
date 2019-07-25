import EventEmitter from 'events';

class Service extends EventEmitter {
  constructor(initialStore) {
    super();
    this.state = initialStore;
  }

  setState(changes) {
    this.state = {
      ...this.state,
      ...changes
    };
    this.emit('change', this.state);
  }

  getAll() {
    return this.state;
  }

  addChangeListener(handler) {
    this.on('change', handler);
  }

  removeChangeListener(handler) {
    this.removeListener('change', handler);
  }

  toJSON() {
    return this.getAll();
  }
}

export default Service;
