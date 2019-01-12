/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const status = document.getElementById('status');
const list = document.getElementById('list');
const inputFile = document.getElementById('file');
const prefix = document.getElementById('prefix');
const uploadBar = document.getElementById('uploadBar');
const uploadInnerBar = uploadBar.children[0];
const uploadStatus = uploadBar.children[1];
const folderLabel = document.getElementById('folder');

function good(msg) {
  status.style.color = 'darkgreen';
  status.innerHTML = msg;
}

function bad(msg, response) {
  status.style.color = 'red';
  if (response.responseText) {
    status.innerHTML = msg + ': ' + response.responseText;
  } else {
    status.innerHTML = msg + ': code ' + response.status;
  }
}

function ajax(url, method, files, uploadProgressListener) {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.onreadystatechange = function () {
      if (this.readyState !== 4) {
        return;
      }
      if (this.status === 200 || this.status === 201) {
        resolve(this);
      } else {
        reject(this);
      }
    };
    if (uploadProgressListener) {
      request.upload.addEventListener('progress', uploadProgressListener);
    }
    request.open(method || 'GET', url, true);
    const fd = new FormData();
    for (const file of (files || [])) {
      fd.append('file', file);
    }
    request.send(fd);
  });
}

// excape the glob metachars for single file operations
function escapeGlobs(name) {
  return name.replace(/([^\\]|^)([*?{}\[\]])/, '$1\\\\$2')
}

// noinspection JSUnusedGlobalSymbols - used from link
function del(file) {
  return ajax(location.pathname + `/gateway/delete?glob=${escapeGlobs(file)}`)
    .then(() => {
      good('Deletion succeeded');
      updateView();
    }, response => bad('Deletion failed', response))
}

// noinspection JSUnusedGlobalSymbols - used from link
function delFolder(folder) {
  return ajax(location.pathname + `/gateway/delete?glob=${escapeGlobs(folder)}/**`)
    .then(() => {
      good('Folder deletion succeeded');
      updateView();
    }, response => bad('Folder deletion failed', response))
}

function getCurrentFolder() {
  const folder = location.hash + '/';
  if (folder.startsWith('#')) {
    return folder.substring(1);
  }
  if (folder === '/') {
    return '';
  }
  return folder;
}

function updateView() {
  status.innerHTML = '';
  const folder = getCurrentFolder();
  if (folder !== '') {
    folderLabel.innerHTML = folder;
    folderLabel.style.display = 'block';
  } else {
    folderLabel.style.display = 'none';
  }
  ajax(location.pathname + `/gateway/list`)
    .then(response => {
      list.innerHTML = '';
      if (folder !== '') {
        const idx = folder.lastIndexOf('/', folder.length - 2);
        const parent = idx === -1 ? '' : '#' + folder.substring(0, idx);
        list.innerHTML += `<div class="box entry"><a href="${location.pathname}${parent}">../</a></div>`;
      }
      const folderSet = {};
      const folders = [];
      const files = [];
      for (const file of JSON.parse(response.responseText)) {
        const name = file[0];
        const size = file[1];
        if (size !== -1 && name.startsWith(folder)) {
          const localName = name.substring(folder.length);
          const idx = localName.indexOf('/');
          if (idx === -1) {
            files.push({name: name, localName: localName, size: size});
            continue;
          }
          const localFolderName = localName.substring(0, idx);
          const folderName = folder + localFolderName;
          if (folderSet[folderName]) {
            continue;
          }
          folderSet[folderName] = true;
          folders.push({name: folderName, localName: localFolderName});
        }
      }
      folders.sort((a, b) => {
        if (a.localName < b.localName) return -1;
        if (a.localName > b.localName) return 1;
        return 0;
      });
      files.sort((a, b) => b.size - a.size);
      for (const f of folders) {
        list.innerHTML +=
          `<div class="box entry">
               <a href="${location.pathname}#${f.name}">${f.localName}/</a>
               <div class="controls">
                   <a href="javascript:delFolder('${f.name}')">delete</a>
               </div>
           </div>`
      }
      for (const f of files) {
        list.innerHTML +=
          `<div class="box entry">
               <a href="${location.pathname}/gateway/download/${f.name}">${f.localName}</a>
               <div class="controls">
                   <div>${formatBytes(f.size)}</div>
                   <a href="javascript:del('${f.name}')">delete</a>
               </div>
           </div>`
      }
    }, response => bad('Could not connect to the Global-FS', response));
}

function formatBytes(bytes) {
  if (bytes === -1) {
    return '&lt;deleted&gt;';
  }
  if (bytes === 0) {
    return '0';
  }
  let sizes = ['b', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  let i = Math.floor(Math.log(bytes) / Math.log(1024));
  return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + sizes[i];
}

document.getElementById('upload').onclick = () => {
  if (inputFile.files.length === 0) {
    alert('Please select a file first');
    return;
  }
  uploadBar.style.display = 'block';
  ajax(location.href + '/gateway/upload/' + getCurrentFolder() + prefix.value, 'POST', inputFile.files, e => {
    let p = parseInt(e.loaded / e.total * 100) + '%';
    uploadInnerBar.style.width = p;
    uploadStatus.innerHTML = p;
  })
    .then(() => {
      good('Upload finished');
      uploadBar.style.display = 'none';
      uploadInnerBar.style.width = '0';
      prefix.value = '';
      updateView();
    }, response => {
      bad('Upload failed', response);
      uploadBar.style.display = 'none';
      uploadInnerBar.style.width = '0';
    })
};

window.onload = () => updateView();
window.onpopstate = () => updateView();
