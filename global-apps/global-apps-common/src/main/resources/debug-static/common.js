function createChildrenHtml(children, prefix) {
  function createLeafHtml(key, children, prefix) {
    if ($.isEmptyObject(children)) {
      return '<div class="row repo" data-repo="' + prefix + key + '">' + key + '</div>';
    }
    return '' +
      '<div class="row has-children" data-path="' + prefix + key + '">' + key + '</div>' +
      '<div class="row collapse">' +
      '<div class="col-auto p-0 px-2"><div class="line"></div></div>' +
      '<div class="col">' +
      createChildrenHtml(children, prefix + key + '/') +
      '</div></div>';
  }

  return Object.entries(children).map(([k, v]) => createLeafHtml(k, v, prefix)).join('');
}

function createTreeList(url, pathCallback, pathFactory,) {
  return fetch(url)
    .then(r => r.json())
    .then(json => {
      if (json.length === 0) {
        return $('<div class="list"><div class="font-italic text-secondary">&lt;empty&gt;</div></div>')
      }

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

      let expandedItems = type + '-list-expanded';

      $list.find('.has-children').click(e => {
        let $e = $(e.currentTarget);
        let $next = $e.next();
        if (!$next.hasClass('collapsing')) {
          $e.toggleClass('expanded');
          $next.collapse('toggle');
          let expanded = JSON.parse(localStorage.getItem(expandedItems) || '[]');
          let path = $e.data('path');
          if ($e.hasClass('expanded')) {
            expanded.push(path)
          } else {
            expanded = expanded.filter(e => e !== path);
          }
          localStorage.setItem(expandedItems, JSON.stringify(expanded));
        }
      });
      $list.find('.repo').click(e => pathCallback($(e.currentTarget).data('repo')));

      let expanded = JSON.parse(localStorage.getItem(expandedItems) || '[]');
      for (let path of expanded) {
        let q = $list.find('[data-path="' + path + '"]');
        q.addClass('expanded').next().addClass('show');
      }
      return $list;
    });
}

function init() {
  $('#' + type + '-link').addClass('active');
  let scrollKey = type + '-scroll';
  $(window).on('beforeunload', () => localStorage.setItem(scrollKey, $(document).scrollTop()));
  let scroll = localStorage.getItem(scrollKey);
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }
}
