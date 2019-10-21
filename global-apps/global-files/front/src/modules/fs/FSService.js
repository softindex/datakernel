import {Service} from 'global-apps-common';
import {ascStringComparator, escapeSpecialChars, PREFIX_TO_IGNORE} from "../../common/utils";

class FSService extends Service {
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
    this.setState({
      loading: true
    });
    const list = await this._globalFSGateway.list();
    const {files, directories} = this._getFilesAndDirectories(list, path);
    this.setState({
      path,
      files,
      directories,
      loading: false
    });
  }

  async writeFile(file) {
    let uploads;

    if (this.state.files.find(({name}) => name === file.name)) {
      throw new Error('File already exists');
    }

    uploads = new Map(this.state.uploads);
    uploads.set(file.name, {
      isDirectory: false,
      name: file.name,
      upload: 0,
      error: null
    });
    this.setState({uploads});

    let downloadURL;
    try {
      downloadURL = await this._globalFSGateway.upload(this.state.path, file, progress => {
        uploads = new Map(this.state.uploads);
        uploads.get(file.name).upload = progress;
        this.setState({uploads});
      });
    } catch (e) {
      uploads = new Map(this.state.uploads);
      uploads.get(file.name).error = e;
      this.setState({uploads});

      throw e;
    }

    const newFile = this.state.uploads.get(file.name);
    newFile.downloadLink = downloadURL;
    this.setState({
      files: [...this.state.files, newFile]
        .sort(ascStringComparator)
    });
  }

  async mkdir(dirName) {
    if (this.state.directories.find(dir => dir.name === dirName)) {
      throw new Error('Such folder exists');
    }

    const file = new File([], PREFIX_TO_IGNORE);
    const fullDirName = this.state.path === '/' ? dirName : this.state.path.match(/^\/?(.*?)\/?$/)[1] + '/' + dirName;

    await this._globalFSGateway.upload(fullDirName, file);

    this.setState({
      directories: [...this.state.directories, {
        isDirectory: true,
        name: dirName,
        upload: 100,
        error: false
      }].sort(ascStringComparator)
    });
  }

  async rmfile(fileName) {
    let path = this.state.path.match(/^\/?(.*?)\/?$/)[1] + '/';
    path = path === '/' ? '' : path;

    const fileToDelete = path + fileName;
    await this._globalFSGateway.removeFile(escapeSpecialChars(fileToDelete));
    this.setState({files: this.state.files.filter(file => file.name !== fileName)});
  }

  async rmdir(dirName = '') {
    let path = this.state.path.match(/^\/?(.*?)\/?$/)[1] + '/';
    path = path === '/' ? '' : path;
    const dirToDelete = escapeSpecialChars(path + dirName) + (dirName ? '/**' : '**');
    await this._globalFSGateway.removeDir(dirToDelete);
    this.setState({
      directories: this.state.directories.filter(directory => directory.name !== dirName)
    });
  }

  clearUploads() {
    this.setState({
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
      let uploads = new Map(this.state.uploads);
      const file = uploads.get(fileName);

      // if it's folder
      if (!file) {
        return;
      }

      file.upload = progress;
      this.setState({uploads});
    });
  }
}

export default FSService;
