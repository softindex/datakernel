import path from 'path';

class GlobalFS {
  constructor(publicKey, url = '/') {
    this._publicKey = publicKey;
    this._url = url;
  }

  async list() {
    const response = await fetch(path.join(this._url, `list/${this._publicKey}`));
    const parsedResponse = await response.json();
    return parsedResponse
      .filter(item => item[2])
      .map(item => ({
        name: item[0],
        size: item[1],
        downloadLink: this._getDownloadLink(item[0])
      }));
  }

  upload(filepath, file, onProgress = null) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.upload.addEventListener('progress', (event) => {
        const progress = Math.round(event.loaded / event.total * 100);
        if (onProgress) {
          onProgress(progress);
        }
      });
      xhr.open('POST', path.join(this._url, 'upload'));
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

      const formData = new FormData();
      const fileName = filepath === '/' ? file.name : filepath + '/' + file.name;
      formData.append('file', file, fileName);

      xhr.send(formData);
    });
  }

  async download() {

  }

  async remove(fileName) {
    await fetch(path.join(this._url, 'delete/?glob=' + fileName), {method: 'POST'});
  }

  _getDownloadLink(filepath) {
    return path.join(this._url, 'download', this._publicKey, filepath);
  }
}

export default GlobalFS;
