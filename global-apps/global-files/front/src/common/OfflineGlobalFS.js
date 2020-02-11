import EventEmitter from 'events';

const MAX_OFFLINE_UPLOAD_SIZE = 1024 * 1024 * 7;

class OfflineGlobalFS extends EventEmitter {
  constructor(globalFS, localforage, navigator) {
    super();
    this._globalFS = globalFS;
    this._localforage = localforage;
    this._navigator = navigator;
  }

  init() {
    this._subscribeForProgress();
  }

  async list() {
    const filesToUpload = await this._getFilesToUpload();
    const cachedFiles = await this._globalFS.list(); // Always returns list because of SW caching
    const cachedNotDeletedFiles = await this._removeDeleted(cachedFiles);
    return [...filesToUpload, ...cachedNotDeletedFiles];
  }

  async upload(filepath, file, onProgress) {
    if (this._navigator.onLine) {
      return await this._globalFS.upload(filepath, file, onProgress);
    }

    if (file.size > MAX_OFFLINE_UPLOAD_SIZE) {
      throw new Error('File is too big for offline uploading');
    }

    const fileName = filepath === '/' ? file.name : filepath + '/' + file.name;
    const duplicated = await this._localforage.getItem(fileName);

    if (duplicated) {
      throw new Error('File is already in list of to be uploaded');
    }

    await this._localforage.setItem(fileName, {type: 'UPLOAD', payload: file});

    this._registerSync();
  }

  async remove(fileName) {
    if (this._navigator.onLine) {
      return await this._globalFS.remove(fileName);
    }

    const fileOperation = await this._localforage.getItem(fileName);

    if (fileOperation) {
      if (fileOperation.type !== 'DELETE') {
        await this._localforage.removeItem(fileName);
      }
      return
    }

    await this._localforage.setItem(fileName, {type: 'DELETE'});

    this._registerSync();
  }

  async removeDir(dirName) {
    if (this._navigator.onLine) {
      return await this._globalFS.removeDir(dirName);
    }

    const dirOperation = await this._localforage.getItem(dirName);

    if (dirOperation) {
      if (dirOperation.type === 'DELETE_FOLDER') {
        throw new Error('Folder is already in list of to be deleted');
      }
      await this._localforage.removeItem(dirOperation);
    } else {
      await this._removeNestedFromDB(dirName);
      await this._localforage.setItem(dirName, {type: 'DELETE_FOLDER'});
    }

    this._registerSync();
  }

  async _getFilesToUpload() {
    const filesToUpload = [];

    await this._localforage.iterate((value, key) => {
      if (value.type === 'UPLOAD') {
        filesToUpload.push({
          isDirectory: false,
          name: key,
          upload: 0,
          error: false
        });
      }
    });

    return filesToUpload.sort(ascStringComparator);
  }

  async _removeDeleted(files) {
    await this._localforage.iterate(async (value, key) => {
      switch (value.type) {
        case 'DELETE': {
          const file = files.find(file => file.name === key);
          const index = files.indexOf(file);
          files.splice(index, 1);
          break;
        }
        case 'DELETE_FOLDER': {
          const folderPath = key.slice(0, key.length - 3);
          files = files.filter(file => {
            return file.name.indexOf(folderPath) !== 0;
          });
          break;
        }
        default:
          throw new Error('Unknown operation type');
      }
    });

    return files;
  }

  _registerSync() {
    this._navigator.serviceWorker.ready.then(swRegistration => {
      return swRegistration.sync.register('connectionExists');
    });
  }

  async _removeNestedFromDB(dirName) {
    // removing glob /**
    const folderPath = dirName.slice(0, dirName.length - 3);
    const deletePromises = [];

    await this._localforage.iterate((value, key) => {
      if (key.indexOf(folderPath) === 0) {
        deletePromises.push(this._localforage.removeItem(key));
      }
    });

    Promise.all(deletePromises);
  };

  _subscribeForProgress() {
    if (this._navigator.onLine) {
      this._globalFS.addListener('progress', ({progress, fileName}) => {
        this.emit('progress', {progress, fileName})
      });
      return;
    }

    const channel = new BroadcastChannel('progress');

    channel.addEventListener('message', ({data: {fileName, progress}}) => {
      this.emit('progress', {progress, fileName});
    });
  }
}

function ascStringComparator(a, b) {
  return a.name.localeCompare(b.name);
}

export default OfflineGlobalFS;
