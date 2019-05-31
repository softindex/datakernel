import EventEmitter from 'events';

class Store extends EventEmitter {
  constructor(initialStore) {
    super();
    this.store = initialStore;
  }

  setStore(store) {
    this.store = {
      ...this.store,
      ...store
    };
    this.emit('change', store);
  }

  getAll() {
    return this.store;
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

export default Store;
