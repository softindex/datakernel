window.onload = () => {
  let $title = $('#title');
  let $body = $('#body');

  init();

  function formatBytes(bytes) {
    if (bytes === 0) return '0 bytes';
    if (bytes === 1) return '1 byte';
    let power = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, power)).toFixed(2) + ' ' + ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'][power];
  }

  function listView() {
    $title.text('FS files');

    function update() {
      let sizes = {};
      createTreeList('/debug/fs/api',
        file => {
          let size = sizes[file];
          if (size && size !== -1) {
            window.open('/debug/fs/' + file);
          }
        },
        meta => {
          sizes[meta[0]] = meta[1];
          return meta[0];
        },
        (path, name) => {
          let bytes = sizes[path];
          return '<div class="col-auto">' + name + '</div>' +
            '<div class="col"></div>' +
            (bytes === -1 ? '<div class="col-auto badge basge-secondary">deleted</div>' : '<div class="col-auto">' + formatBytes(bytes) + '</div>');
        })
        .then($list => $body.empty().append($('<div class="container"></div>').append($list)), console.log);
    }

    update();
    setInterval(update, 3000);
  }

  listView(location.pathname.substring('/debug/fs'.length).replace(/^\/|\/$/g, ''), false);
};
