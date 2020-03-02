import path from 'path';
import EventEmitter from 'events';

class GlobalFS extends EventEmitter {
  constructor(url = '/fs/') {
    super();
    this._url = url;
  }

  async list(dirPath) {
    const response = await fetch(path.join(this._url, 'shallowList?dir=' + dirPath));
    const parsedResponse = await response.json();
    return parsedResponse
      .map(({name, isDirectory}) => ({
        name,
        isDirectory,
        downloadLink: this._getDownloadLink(dirPath + '/' + name)
      }));
  }

  upload(filepath, file) {
    const formData = new FormData();
    const fileName = filepath === '' ? file.name : filepath + '/' + file.name;
    const url = path.join(this._url, 'upload');
    formData.append('file', file, fileName);

    if (typeof window === 'undefined') {
      return fetch(url, {
        method: 'POST',
        body: formData
      })
        .then(() => {
          this.emit('progress', {progress: 100, fileName});
        });
    }

    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.upload.addEventListener('progress', (event) => {
        const progress = Math.round(event.loaded / event.total * 100);
        this.emit('progress', {progress, fileName});
      });
      xhr.open('POST', path.join(this._url, 'upload?revision=' + GlobalFS.getRevision()));
      xhr.onreadystatechange = () => {
        if (xhr.readyState === 4) {
          // if there is an error
          if (xhr.status === 200) {
            resolve(this._getDownloadLink(fileName));
          } else {
            reject(new Error('Failed request. Status code: ' + xhr.status));
          }
        }
      };

      xhr.send(formData);
    });
  }

  async removeDir(dirName) {
    await fetch(path.join(this._url, 'deleteBulk?glob=' + dirName), {method: 'POST'});
  }

  async remove(fileName) {
    await fetch(path.join(this._url, 'delete/' + fileName + '?revision=' + GlobalFS.getRevision()), {method: 'POST'});
  }

  _getDownloadLink(filepath) {
    return path.join(this._url, 'download', filepath);
  }

  static getRevision() {
    return new Date().getTime();
  }
}

export default GlobalFS;
