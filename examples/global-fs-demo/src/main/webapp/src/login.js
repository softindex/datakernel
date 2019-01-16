const $ = require('jquery');

const repos = JSON.parse(localStorage.getItem('repos')) || {};
const repoList = $('#repos');

function updateRepos() {
  $.ajax('/listRepos')
    .then(repos => {
      repoList.children().remove();
      for (const pk of repos) {
        repoList.append($(`<div class="box knownPK">${pk}</div>`)
          .click(() => {
            location.pathname = '/' + pk;
          }));
      }
    });
}

updateRepos();

$('#newRepo').click(() => {
  $.ajax('/newRepo')
    .then(keys => {
      repos[keys[1]] = keys[0];
      localStorage.setItem('repos', JSON.stringify(repos));
      updateRepos();
    })
});

$('#go').click(() => {
  location.pathname = '/' + $('#pubkey').get(0).value;
});
