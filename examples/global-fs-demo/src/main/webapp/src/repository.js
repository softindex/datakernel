const $ = require('jquery');
const prettyByte = require('pretty-byte');

const status = $('#status');
const list = $('#list');
const inputFile = $('#file').get(0);
const prefix = $('#prefix');
const offset = $('#offset').get(0);
const from = $('#from').get(0);
const to = $('#to').get(0);
const folderLabel = $('#folder');
const bars = $('#bars');
const keySelect = $('#key');

let knownKeys = {};

function good(msg) {
  status.css('color', 'darkgreen')
    .html(msg);
}

function bad(msg, response) {
  status
    .css('color', 'red')
    .html(msg + (response ? (response.responseText ? ': ' + response.responseText : ': code ' + response.status) : ''));
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

function updateKeys() {
  $.ajax('/listKeys')
    .then(res => {
      keySelect.children().remove();
      knownKeys = {};
      keySelect.append('<option value="&lt;none&gt;">&lt;none&gt;</option>');
      for (const [name, hash] of res) {
        knownKeys[hash] = name;
        keySelect.append(`<option value="${name}">${name}</option>`);
      }
      let guard = false;
      keySelect.get(0).value = localStorage.getItem('selectedKey');
      keySelect
        .append('<option value="&lt;new&gt;">&lt;generate new&gt;</option>')
        .change(() => {
          if (guard) {
            return;
          }
          if (keySelect.get(0).value === '<new>') {
            const name = prompt('Please, enter a name for the new key');
            if (name !== null) {
              $.ajax(`/newKey/${name}`)
                .then(() => {
                  good('Key ' + name + ' generated and added');
                  localStorage.setItem('selectedKey', name);
                  updateKeys();
                }, response => bad('Key generation failed', response));
            }
            return;
          }
          const name = keySelect.get(0).value;
          $.ajax(`/setKey/${name}`)
            .then(() => {
              good(`Key ${name} selected`);
              localStorage.setItem('selectedKey', name);
              updateKeys();
            })
        });
    });
}

function updateView(keepBars) {
  status.empty();
  if (!keepBars) {
    bars.children().remove();
  }

  updateKeys();

  const folder = getCurrentFolder();
  if (folder !== '') {
    folderLabel.html(folder).css('display', 'block');
  } else {
    folderLabel.css('display', 'none');
  }
  $.ajax(`${location.pathname}/list`)
    .then(res => {
      const folderSet = {};
      const folders = [];
      const files = [];
      for (const [name, size, sha256, encryptionKeyHash] of res) {
        if (sha256 === null || !name.startsWith(folder)) {
          continue;
        }
        const localName = name.substring(folder.length);
        const idx = localName.indexOf('/');
        if (idx === -1) {
          files.push({
            name: name,
            localName: localName,
            size: size,
            encryptionKeyHash: encryptionKeyHash,
            sha256: sha256
          });
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
      folders.sort((a, b) => a.localName < b.localName ? -1 : a.localName > b.localName ? 1 : 0);
      files.sort((a, b) => b.size - a.size);

      list.empty();
      if (folder !== '') {
        const idx = folder.lastIndexOf('/', folder.length - 2);
        const parent = idx === -1 ? '' : '#' + folder.substring(0, idx);
        list.append(`<div class="box entry"><a href="${location.pathname}${parent}">../</a></div>`);
      }
      for (const f of folders) {
        const box = $('<div class="box entry"></div>');
        const panel = $('<div class="file-panel"></div>');
        const del = $('<div class="control">delete</div>')
          .click(() =>
            $.ajax(`${location.pathname}/gateway/delete?glob=${f.name}/**`, {method: 'POST'})
              .then(() => {
                updateView();
                good('Folder deletion succeeded');
              }, response => bad('Folder deletion failed', response)));

        panel.append(del);
        box.append(`<a href="${location.pathname}#${f.name}">${f.localName}/</a>`);
        box.append(panel);
        list.append(box);
      }
      for (const f of files) {
        const box = $('<div class="box entry"></div>');
        const panel = $('<div class="file-panel"></div>');
        const del = $('<div class="control">delete</div>')
          .click(() =>
            $.ajax(`${location.pathname}/gateway/delete/${f.name}`, {method: 'POST'})
              .then(() => {
                updateView();
                good('Deletion succeeded');
              }, response => bad('Deletion failed', response)));

        if (f.encryptionKeyHash) {
          const name = knownKeys[f.encryptionKeyHash];
          panel.append(`<div class="encrypted">encrypted${name ? `: ${name}` : ''}</div>`);
        }
        panel.append(`<div class="size" title="${f.size.toString().replace(/\\\\B(?=(\\\\d{3})+(?!\\\\d))/g, " ")} bytes">${prettyByte(f.size)}</div>`);
        panel.append(del);
        box.append(`<a href="${location.pathname}/gateway/download/${f.name}" title="SHA256: ${f.sha256}">${f.localName}</a>`);
        box.append(panel);
        list.append(box);
      }
    }, response => bad('Could not connect to the Global-FS', response));
}

function createProgressBar(filename) {
  const elem = $('<div class="progress"></div>');
  const border = $(`<div class="border"></div>`);
  const bar = $('<div class="bar"></div>');
  const percentage = $('<div class="percentage">0%</div>');
  const cancel = $('<div class="control">cancel</div>');
  const status = $('<div class="status"></div>');
  status.html(filename);
  border.append(bar);
  border.append(percentage);
  elem.append(border);
  elem.append(cancel);
  elem.append(status);
  bars.append(elem);
  return {
    set: (p) => {
      percentage.html(p.toFixed(1) + '%');
      bar.css('width', p + '%');
    },
    error: msg => {
      status.css('color', 'red').html(msg);
      cancel.html('clear').click(() => elem.remove());
    },
    remove: () => elem.remove(),
    onCancel: callback => cancel.click(callback)
  }
}

const repo = location.pathname.substring(1);
const key = JSON.parse(localStorage.getItem('repos') || '{}')[repo];
if (key) {
  document.cookie = `key=${key}`;
}

$('#pubkey').html(repo);

$('#upload').click(() => {
  if (inputFile.files.length === 0) {
    bad('Please select a file first');
    return;
  }
  const start = from.valueAsNumber;
  const limit = to.valueAsNumber;
  if (inputFile.files.length !== 1 && (offset.valueAsNumber > 0 || start !== 0 || limit !== -1)) {
    bad('Cannot upload multiple files partially (remote offset is not -1 or 0, offset is not 0 and/or limit is not -1)');
    return;
  }
  if (offset.valueAsNumber < -1) {
    bad('Remote offset value cannot be less than -1');
    return;
  }
  if (start < 0) {
    bad('Source start cannot be less than 0');
    return;
  }
  if (limit < -1) {
    bad('Limit cannot be less than -1');
    return;
  }
  updateView();

  const pref = prefix.get(0).value;
  let running = inputFile.files.length;
  let errors = 0;

  function check() {
    if (--running) {
      return;
    }
    prefix.empty();
    updateView(true);
    if (errors) {
      bad(`${errors} uploads failed`);
    }
  }

  for (const file of inputFile.files) {
    const bar = createProgressBar(file.name);
    const request = new XMLHttpRequest();
    request.onreadystatechange = function () {
      if (this.readyState !== 4) {
        return;
      }
      if (this.status === 200 || this.status === 201) {
        bar.remove();
      } else {
        errors++;
        bar.error(`upload of '${file.name}' failed: ${this.responseText ? this.responseText : 'code ' + this.status}`);
      }
      check();
    };
    request.upload.addEventListener('progress', e => bar.set(e.loaded / e.total * 100));
    const filename = getCurrentFolder() + pref + (pref === '' || pref.endsWith('/') ? '' : '/') + file.name;
    request.open('POST', `${location.pathname}/gateway/upload${(offset.valueAsNumber !== -1 ? `?offset=${offset.valueAsNumber}` : '')}`, true);
    const fd = new FormData();
    fd.append('file', start <= 0 && limit === -1 ? file : file.slice(start, limit === -1 ? file.size : start + limit), filename);
    request.send(fd);
    bar.onCancel(() => {
      request.abort();
      bar.error(`upload of '${file.name}' cancelled`);
      check();
    });
  }
});

window.onpopstate = () => updateView(true);
updateView();
