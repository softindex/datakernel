class LocalFS {
  constructor() {
    this._queue = [];
  }

  async list() {
    return this._queue;
  }

  upload(filepath, file, onProgress) {
  }

  async download() {

  }

  async deleteFile(fileName) {
  }
}

export default LocalFS;
