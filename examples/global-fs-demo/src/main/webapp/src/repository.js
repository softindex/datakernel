const $ = require('jquery');
const hash = require('hash.js');
const prettyByte = require('pretty-byte');
const cookies = require('js-cookie');

const status = $('#status');
const list = $('#list');
const inputFile = $('#file');
const prefix = $('#prefix');
const offset = $('#offset');
const from = $('#from');
const to = $('#to');
const folderLabel = $('#folder');
const bars = $('#bars');
const keySelect = $('#key');

const space = localStorage.getItem('space');
const privKey = JSON.parse(localStorage.getItem('repos') || '{}')[space];
const keys = JSON.parse(localStorage.getItem('keys') || '{}');

$('#pubkey').html(space);
if (privKey) {
  cookies.set('Key', privKey);
  const selectedKey = (keys[localStorage.getItem('selectedKey') || ''] || {}).key;
  if (selectedKey) {
    cookies.set('Sim-Key', selectedKey);
  }
} else {
  $('#upload-panel').hide();
  cookies.remove('Sim-Key');
}

const extended = $('#extended').hide();

$('#extend').change(e => {
  console.log(e.target.checked);
  if (e.target.checked) {
    extended.show();
  } else {
    extended.hide();
  }
});

function good(msg) {
  status
    .css('color', 'darkgreen')
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
  keySelect.children().remove();
  keySelect.append('<option value="_#none#_">&lt;none&gt;</option>');

  for (const hash of Object.keys(keys)) {
    keySelect.append(`<option value="${hash}">${keys[hash].name}</option>`);
  }
  let guard = false;
  keySelect
    .append('<option value="_#new#_">&lt;generate new&gt;</option>')
    .val(localStorage.getItem('selectedKey') || '_#none#_')
    .change(() => {
      if (guard) {
        return;
      }
      const selected = keySelect.val();
      if (selected === '_#none#_') {
        localStorage.removeItem('selectedKey');
        cookies.remove('Sim-Key');
        return
      }
      if (selected !== '_#new#_') {
        localStorage.setItem('selectedKey', selected);
        const key = (keys[selected] || {}).key;
        if (key) {
          cookies.set('Sim-Key', key);
        }
        return;
      }
      guard = true;
      keySelect.val('_#none#_');
      guard = false;
      const name = prompt('Please, enter a name for the new key');
      if (!name) {
        return;
      }
      const [key, hash] = generateSimKey();
      good('New key generated and stored as \'' + name + '\'');
      keys[hash] = {name, key};
      localStorage.setItem('selectedKey', hash);
      localStorage.setItem('keys', JSON.stringify(keys));
      cookies.set('Sim-Key', key);
      guard = true;
      updateKeys();
      keySelect.val(hash);
      guard = false;
    });
}

function generateSimKey() {
  const array = window.crypto.getRandomValues(new Uint8Array(16));
  const key = Array.from(array).map(b => b.toString(16).padStart(2, "0")).join("");
  const hash = hash.sha1().update(array).digest('hex');
  return [key, hash];
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
  $.ajax(`/list/${space}`)
    .then(res => {
      const folderSet = {};
      const folders = [];
      const files = [];
      for (const [name, size, revision, sha256, encryptionKeyHash] of res) {
        if (sha256 === null || !name.startsWith(folder)) {
          continue;
        }
        const localName = name.substring(folder.length);
        const idx = localName.indexOf('/');
        if (idx === -1) {
          files.push({name, localName, size, sha256, encryptionKeyHash});
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
            $.ajax(`/delete?glob=${f.name}/**`, {method: 'POST'})
              .then(() => {
                updateView();
                good('Folder deletion succeeded');
              }, response => bad('Folder deletion failed', response)));

        panel.append(del);
        box.append(`<a href="#${f.name}">${f.localName}/</a>`);
        box.append(panel);
        list.append(box);
      }
      for (const f of files) {
        const box = $('<div class="box entry"></div>');
        const panel = $('<div class="file-panel"></div>');
        const del = $('<div class="control">delete</div>')
          .click(() =>
            $.ajax(`/delete/${f.name}?revision=${new Date().getTime()}`, {method: 'POST'})
              .then(() => {
                updateView();
                good('Deletion succeeded');
              }, response => bad('Deletion failed', response)));

        if (f.encryptionKeyHash) {
          const data = keys[f.encryptionKeyHash];
          panel.append(`<div class="encrypted" title="SHA1: ${f.encryptionKeyHash}">encrypted${data ? `: ${data.name}` : ''}</div>`);
        }
        panel.append(`<div class="size" title="${f.size.toString().replace(/\\\\B(?=(\\\\d{3})+(?!\\\\d))/g, " ")} bytes">${f.size === 0 ? '0 b' : prettyByte(f.size)}</div>`);
        panel.append(del);
        box.append(`<a href="/download/${space}/${f.name}" title="SHA256: ${f.sha256}">${f.localName}</a>`);
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

$('#upload').click(() => {
  if (!privKey) {
    bad('You dont have the private key required to upload files');
    return;
  }
  const files = inputFile.prop('files');
  if (files.length === 0) {
    bad('Please select a file first');
    return;
  }
  const off = parseInt(offset.val());
  const start = parseInt(from.val());
  const limit = parseInt(to.val());
  if (files.length !== 1 && (off !== 0 || start !== 0 || limit !== -1)) {
    bad('Cannot upload multiple files partially (remote offset is 0, offset is not 0 and/or limit is not -1)');
    return;
  }
  if (off < 0) {
    bad('Remote offset value cannot be less than 0');
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

  const pref = prefix.val();
  let running = files.length;
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

  for (const file of files) {
    const filename = getCurrentFolder() + pref + (pref === '' || pref.endsWith('/') ? '' : '/') + file.name;
    const bar = createProgressBar(file.name);
    const xhr = new XMLHttpRequest();
    xhr.onreadystatechange = function () {
      if (this.readyState !== 4) {
        return;
      }
      if (this.status === 200) {
        bar.remove();
      } else {
        errors++;
        bar.error(`upload of '${file.name}' failed: ${this.responseText ? this.responseText : 'code ' + this.status}`);
      }
      check();
    };
    xhr.upload.addEventListener('progress', e => bar.set(e.loaded / e.total * 100));
    xhr.open('POST', `/upload?revision=${new Date().getTime()}${(off !== 0 ? `&offset=${off}` : '')}`, true);
    const fd = new FormData();
    fd.append('file', start <= 0 && limit === -1 ? file : file.slice(start, limit === -1 ? file.size : start + limit), filename);
    xhr.send(fd);
    bar.onCancel(() => {
      xhr.abort();
      bar.error(`upload of '${file.name}' cancelled`);
      check();
    });
  }
});

window.onpopstate = () => updateView(true);
updateView();
