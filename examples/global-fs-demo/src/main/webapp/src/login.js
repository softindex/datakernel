const $ = require('jquery');
const elliptic = require('elliptic');

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
  const [pk, sk] = generateKeyPair();
  repos[pk] = sk;
  localStorage.setItem('repos', JSON.stringify(repos));
  pubkey.val(pk);
  updateRepos();
});

$('#go').click(() => {
  localStorage.setItem('space', pubkey.val());
  location.pathname = '/view';
});

const ec = new elliptic.ec('secp256k1');

function generateKeyPair() {
  const key = ec.genKeyPair();
  const point = key.getPublic();
  return [`${point.getX().toString('hex')}:${point.getY().toString('hex')}`, key.getPrivate().toString('hex')]
}
