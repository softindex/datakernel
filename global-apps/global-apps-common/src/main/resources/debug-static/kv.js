window.onload = () => {
  let $title = $('.title');
  let $key = $('.key');
  let $body = $('.body');

  function tableView(pk, repo) {
    $title.text('Contents of table:');
    $key.text(repo);

    fetch('/debug/kv/api/' + pk + '/' + repo)
      .then(r => r.json())
      .then(json => {

        let $tbody = $('<tbody></tbody>');
        for (let [k, v, t] of json) {
          $tbody.append($('<tr>')
            .append($('<td>').text(k))
            .append($('<td>').text(v))
            .append($('<td>').text($.format.date(t, 'yyyy-MM-dd/HH:mm:ss'))))
        }

        $body.empty()
          .append($('<table class="table table-striped table-sm"><thead><tr><th>key</th><th>value</th><th>timestamp</th></tr></thead></table>').append($tbody));

      }, console.log);
  }

  function listView(pk) {
    function update() {
      $title.text('KV tables of:');
      $key.text(pk);

      createTreeList('/debug/kv/api/' + pk, repo => view(pk + '/' + repo))
        .then($list => $body.empty().append($('<div class="container"></div>').append($list)), console.log);
    }

    update();
  }

  function view(path, push = true) {
    let [pk, ...tail] = path.split('/');
    if (tail.length === 0) {
      listView(pk);
    } else {
      tableView(pk, tail.join('/'));
    }
    if (push) {
      window.history.pushState(null, null, '/debug/kv/' + path);
    }
  }

  function pathView() {
    let path = location.pathname;
    view(path.substring('/debug/kv/'.length, path.endsWith('/') ? path.length - 1 : undefined), false);
  }

  pathView();

  window.onpopstate = pathView;
};
