function createChildrenHtml(children, prefix) {
  return Object.entries(children).map(([k, v]) => createLeafHtml(k, v, prefix)).join('');
}

function createLeafHtml(key, children, prefix) {
  if ($.isEmptyObject(children)) {
    return '<div class="row repo" data-repo="' + prefix + key + '">' + key + '</div>';
  }
  return '' +
    '<div class="row has-children">' + key + '</div>' +
    '<div class="row collapse">' +
    '<div class="col-auto p-0 px-2"><div class="line"></div></div>' +
    '<div class="col">' +
    createChildrenHtml(children, prefix + key + '/') +
    '</div></div>';
}

function createTreeList(url, repoCallback, pathFactory=x=>x) {
  return fetch(url)
    .then(r => r.json())
    .then(json => {
      let stuff = {};

      for (let item of json) {
        item = pathFactory(item);
        let nested = stuff;
        for (let part of item.split('/')) {
          if (part in nested) {
            nested = nested[part];
            continue;
          }
          let next = {};
          nested[part] = next;
          nested = next;
        }
      }

      let $list = $('<div class="list">' + createChildrenHtml(stuff, '') + '</div>');

      $list.find('.has-children').click(e => {
        let $e = $(e.currentTarget);
        let $next = $e.next();
        if (!$next.hasClass('collapsing')) {
          $e.toggleClass('expanded');
          $next.collapse('toggle');
          $next.find('.collapse').collapse('hide').prev().removeClass('expanded');
        }
      });
      $list.find('.repo').click(e => repoCallback($(e.currentTarget).data('repo')));

      return $list;
    });
}
