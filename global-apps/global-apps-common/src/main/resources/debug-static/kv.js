window.onload = () => {
  let $title = $('#title');
  let $body = $('#body');
  let $polling = $('#polling');
  let $pollingCb = $polling.find('input:first');

  let $collapses = $('.collapse');
  let $nonDefault = $('.non-default');

  let pollTimerId = null;
  let listTimerId = null;

  init();

  function tableView(table) {
    $title.text('Contents of table \'' + table + '\'');
    $polling.show();

    function update() {
      fetch('/debug/kv/api/' + table)
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

    update();

    function pollTimer() {
      update();
      pollTimerId = setTimeout(pollTimer, 1000);
    }

    let pollingDisabled = localStorage.getItem(type + '-no-polling');

    $pollingCb.prop('checked', !pollingDisabled);

    if (pollingDisabled) {
      update();
    } else {
      pollTimer();
    }

    $pollingCb.off('click').click(() => {
      if (pollTimerId) {
        clearTimeout(pollTimerId);
        pollTimerId = null;
        localStorage.setItem(type + '-no-polling', 'true');
      } else {
        pollTimer();
        localStorage.removeItem(type + '-no-polling');
      }
    });
  }

  function listView() {
    $title.text('KV tables');

    function update() {
      createTreeList('/debug/kv/api', table => view(table, true), x => x)
        .then($list => $body.empty().append($('<div class="container"></div>').append($list)), console.log);
    }

    update();
    listTimerId = setInterval(update, 3000);
  }

  function view(path, push) {
    $collapses.removeClass('show');
    $nonDefault.hide();
    $body.empty();

    if (pollTimerId) {
      clearTimeout(pollTimerId);
      pollTimerId = null;
    }
    if (listTimerId) {
      clearInterval(listTimerId);
      listTimerId = null;
    }

    if (path === '') {
      listView();
    } else {
      tableView(path);
    }
    if (push) {
      window.history.pushState(null, null, '/debug/kv/' + path);
    }
  }

  function pathView() {
    view(location.pathname.substring('/debug/kv'.length).replace(/^\/|\/$/g, ''));
  }

  pathView();

  window.onpopstate = pathView;
};
