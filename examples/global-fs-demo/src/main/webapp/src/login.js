const $ = require('jquery');

const repos = JSON.parse(localStorage.getItem('repos') || '{}');
const repoList = $('#repos');

function updateRepos() {
  repoList.children().remove();
  for (const pk of Object.keys(repos)) {
    repoList.append($(`<div class="box knownPK">${pk}</div>`)
      .click(() => {
        localStorage.setItem('viewing', pk);
        location.pathname = '/view';
      }));
  }
}

updateRepos();

const pubkey = $('#pubkey');
pubkey.val(localStorage.getItem('pubkeyField') || '');
pubkey.on('input', () => localStorage.setItem('pubkeyField', pubkey.val()));

$('#generate').click(() => {
  $.ajax('/generateKeyPair')
    .then(keys => {
      repos[keys[1]] = keys[0];
      localStorage.setItem('repos', JSON.stringify(repos));
      pubkey.val(keys[1]);
      updateRepos();
    })
});

$('#go').click(() => {
  localStorage.setItem('viewing', pubkey.val());
  location.pathname = '/view';
});
