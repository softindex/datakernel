window.onload = () => {
  let $title = $('.title');
  let $key = $('.key');
  let $body = $('.body');

  function listView(pk) {
    function update() {
      $title.text('FS files of:');
      $key.text(pk);

      createTreeList('/debug/fs/api/' + pk, file => window.open('/debug/fs/' + pk + '/' + file), meta => meta[0])
        .then($list => $body.empty().append($('<div class="container"></div>').append($list)), console.log);
    }

    update();
  }

  let path = location.pathname;
  listView(path.substring('/debug/fs/'.length, path.endsWith('/') ? path.length - 1 : undefined), false);
}
