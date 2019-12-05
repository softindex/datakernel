window.onload = () => {
  let $title = $('#title');
  let $body = $('#body');

  init();

  function listView() {
    $title.text('FS files');

    function update() {
      createTreeList('/debug/fs/api', file => window.open('/debug/fs/' + file), meta => meta[0])
        .then($list => $body.empty().append($('<div class="container"></div>').append($list)), console.log);
    }

    update();
    setInterval(update, 3000);
  }

  listView(location.pathname.substring('/debug/fs'.length).replace(/^\/|\/$/g, ''), false);
};
