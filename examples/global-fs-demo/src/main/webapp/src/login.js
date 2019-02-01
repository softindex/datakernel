const $ = require('jquery');
const cookies = require('js-cookie');

const repos = JSON.parse(localStorage.getItem('repos') || '{}');
const repoList = $('#repos');

function updateRepos() {
  repoList.children().remove();
  for (const pk of Object.keys(repos)) {
    repoList.append($('<div class="box knownPK"></div>')
      .append(`<div style="margin: auto">${pk}</div>`)
      .append($('<div class="control">forget</div>')
        .click(() => {
          delete repos[pk];
          localStorage.setItem('repos', JSON.stringify(repos));
          updateRepos();
        }))
      .click(() => {
        localStorage.setItem('space', pk);
        location.pathname = '/view';
      }));
  }
}

updateRepos();

const pubkey = $('#pubkeyField');
pubkey.val(localStorage.getItem('pubkeyField') || '');
pubkey.on('input', () => localStorage.setItem('pubkeyField', pubkey.val()));

$('#generate').click(() => {
  $.ajax('/genKeyPair')
    .then(keys => {
      repos[keys[1]] = keys[0];
      localStorage.setItem('repos', JSON.stringify(repos));
      pubkey.val(keys[1]);
      updateRepos();
    })
});

$('#go').click(() => {
  localStorage.setItem('space', pubkey.val());
  location.pathname = '/view';
});
