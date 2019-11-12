import Store from '../../common/Store';

const PREFIX_TO_IGNORE = '.~#!HIDDEN!#~.';

function ascStringComparator(a, b) {
  return a.name.localeCompare(b.name);
}

function escapeSpecialChars(unsafe) {
  return unsafe
    .replace(/\*/g, '\\*')
    .replace(/\?/g, '\\?')
    .replace(/\{/g, '\\{')
    .replace(/\}/g, '\\}')
    .replace(/\[/g, '\\[')
    .replace(/\]/g, '\\]')
}

class FSService extends Store {
  constructor(globalFSGateway) {
    super({
      files: [],
      directories: [],
      uploads: new Map(),
      path: '',
      loading: true
    });
    this._globalFSGateway = globalFSGateway;
    this._subscribeForProgress();
  }

  async fetch(path) {
    this.setStore({
      loading: true
    });
    const list = await this._globalFSGateway.list();
    const {files, directories} = this._getFilesAndDirectories(list, path);
    this.setStore({
      path,
      files,
      directories,
      loading: false
    });
  }

  async writeFile(file) {
    let uploads;

    if (this.store.files.find(({name}) => name === file.name)) {
      throw new Error('File already exists');
    }

    uploads = new Map(this.store.uploads);
    uploads.set(file.name, {
      isDirectory: false,
      name: file.name,
      upload: 0,
      error: null
    });
    this.setStore({uploads});

    let downloadURL;
    try {
      downloadURL = await this._globalFSGateway.upload(this.store.path, file, progress => {
        uploads = new Map(this.store.uploads);
        uploads.get(file.name).upload = progress;
        this.setStore({uploads});
      });
    } catch (e) {
      uploads = new Map(this.store.uploads);
      uploads.get(file.name).error = e;
      this.setStore({uploads});

      throw e;
    }

    const newFile = this.store.uploads.get(file.name);
    newFile.downloadLink = downloadURL;
    this.setStore({
      files: [...this.store.files, newFile]
        .sort(ascStringComparator)
    });
  }

  async mkdir(dirName) {
    if (this.store.directories.find(dir => dir.name === dirName)) {
      throw new Error('Such folder exists');
    }

    const file = new File([], PREFIX_TO_IGNORE);
    const fullDirName = this.store.path === '/' ? dirName : this.store.path.match(/^\/?(.*?)\/?$/)[1] + '/' + dirName;

    await this._globalFSGateway.upload(fullDirName, file);

    this.setStore({
      directories: [...this.store.directories, {
        isDirectory: true,
        name: dirName,
        upload: 100,
        error: false
      }].sort(ascStringComparator)
    });
  }

  async rmfile(fileName) {
    // formatting path to 'foo/bar/'
    let path = this.store.path.match(/^\/?(.*?)\/?$/)[1] + '/';
    path = path === '/' ? '' : path;

    const fileToDelete = path + fileName;
    await this._globalFSGateway.remove(escapeSpecialChars(fileToDelete));
    this.setStore({files: this.store.files.filter(file => file.name !== fileName)});
  }

  async rmdir(dirName = '') {
    // formatting path to 'foo/bar/'
    let path = this.store.path.match(/^\/?(.*?)\/?$/)[1] + '/';
    path = path === '/' ? '' : path;
    const dirToDelete = escapeSpecialChars(path + dirName) + (dirName ? '/**' : '**');
    await this._globalFSGateway.removeDir(dirToDelete);
    this.setStore({
      directories: this.store.directories.filter(directory => directory.name !== dirName)
    });
  }

  clearUploads() {
    this.setStore({
      uploads: []
    });
  }

  _getFilesAndDirectories(list, path) {
    // Formatting path to 'foo/bar/'
    path = path.match(/^\/?(.*?)\/?$/)[1] + '/';
    path = path === '/' ? '' : path;

    const files = [];
    const directoriesSet = new Set();

    for (const {name, downloadLink} of list) {
      if (name.indexOf(path) !== 0) {
        continue;
      }

      let nextSlashPos = name.indexOf('/', path.length);
      if (nextSlashPos !== -1) {
        directoriesSet.add(name.slice(path.length, nextSlashPos));
        continue;
      }

      const fileName = name.slice(path.length);
      if (fileName.indexOf(PREFIX_TO_IGNORE) !== 0) {
        files.push({
          isDirectory: false,
          name: fileName,
          upload: 100,
          error: false,
          downloadLink
        });
      }
    }

    return {
      files: files.sort(ascStringComparator),
      directories: [...directoriesSet]
        .map(directory => ({
          isDirectory: true,
          name: directory,
          upload: 100,
          error: false
        }))
        .sort(ascStringComparator)
    };
  }

  _subscribeForProgress() {
    this._globalFSGateway.addListener('progress', ({progress, fileName}) => {
      let uploads = new Map(this.store.uploads);
      const file = uploads.get(fileName);

      // if it's folder
      if (!file) {
        return;
      }

      file.upload = progress;
      this.setStore({uploads});
    });
  }
}

export default FSService;
