window.onload = () => {
  let $title = $('#title');
  let $body = $('#body');
  let $diffModal = $('.diff-modal');
  let $graphCoords = $('#graph-coords');
  let $polling = $('#polling');
  let $pollingCb = $polling.find('input:first');

  let $collapses = $('.collapse');
  let $nonDefault = $('.non-default');

  let $svg = null;
  let pollTimerId = null;
  let listTimerId = null;

  init();
  $diffModal.find('.diff-hide').click(() => $diffModal.collapse('hide'));

  let viz = new Viz();

  function updateGraph(repo) {
    fetch('/debug/ot/api/' + repo)
      .then(r => r.text())
      .then(text => {
        return viz.renderSVGElement(text.replace(/label="([^"]*?)"/g, (_, content) => {
          let diffs = content.split(',\n');
          let label = diffs.map(d => d.split('|')[0]).join('\n+');
          let xlabel = diffs.map(d => d.split('|')[1]).join('\n');
          return 'label="+' + label + '"; xlabel="' + xlabel + '"';
        }));
      })
      .then(svg => {
        let pos = [0, 0];
        let scale = 1;
        if ($svg) {
          pos = $svg.data('pos') || pos;
          scale = $svg.data('scale') || scale;
        }

        $svg = $(svg);

        let diffMap = {};

        $svg.find('g.edge > text').each((_, e) => {
          let $e = $(e);
          let text = $e.text();
          if (text.startsWith('+')) {
            $e.text(text.substring(1)).attr('font-family', 'sans-serif').attr('font-size', '10');
            return;
          }
          let edge = $e.parent().attr('id');
          let diffs = diffMap[edge];
          if (!diffs) {
            diffs = [];
            diffMap[edge] = diffs;
          }
          diffs.push(text);
          $e.remove();
        });

        $svg.find('g.graph > title').remove(); // remove the %0 global title (idk what is this)
        $svg.find('g.graph > polygon').remove(); // remove white background

        let $graph = $svg.find('g.graph');
        $graph.attr('transform', 'scale(1.5)');

        $svg.find('g.edge').click(e => {
          let id = e.currentTarget.id;
          if (id === $diffModal.data('edge') && $diffModal.hasClass('show')) {
            $diffModal.collapse('hide');
            return
          }
          $diffModal.removeClass('show');
          $diffModal.data('edge', id);
          $diffModal.find('.diff-title-content').text('Diffs for: ' + $(e.currentTarget).find('title').text());
          $diffModal.find('.diff-body').empty()
            .append(diffMap[id].map(diff => $('<div class="diff"></div>').html(diff.replace(/UserId{pl='([^']*)'}/g, '<div class="user-id" title="$1">USER</div>'))));

          let rect = e.currentTarget.getBoundingClientRect();
          $diffModal.css('left', rect.x - $diffModal.width() / 2);
          $diffModal.css('top', rect.y + rect.height / 2 + $(window).scrollTop());
          $diffModal.collapse('show');
        });

        $body.empty().append($('<div class="mx-auto mt-5"></div>').append($svg));

        let bbox = $svg[0].getBBox();
        $svg.attr('width', bbox.width + 2);
        $svg.attr('height', bbox.height + 2);
        $svg.attr('viewBox', [bbox.x - 2, bbox.y - 2, bbox.width + 4, bbox.height + 4].join(' '));

        transform(pos[0], pos[1], scale)
      }, e => {
        viz = new Viz();
        console.error(e);
      })
  }

  function graphView(repo) {
    $title.text('Commit graph of repository \'' + repo + '\'');
    $graphCoords.show();
    $polling.show();

    document.body.style.overflow = 'hidden';

    function pollTimer() {
      updateGraph(repo);
      pollTimerId = setTimeout(pollTimer, 1000);
    }

    let pollingDisabled = localStorage.getItem(type + '-no-polling');

    $pollingCb.prop('checked', !pollingDisabled);

    if (pollingDisabled) {
      updateGraph(repo);
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
    $title.text('OT repositories');

    function update() {
      createTreeList('/debug/ot/api', repo => view(repo, true), x => x)
        .then($list => $body.empty().append($('<div class="container"></div>').append($list)), console.log);
    }

    update();
    listTimerId = setInterval(update, 3000);
  }

  function view(path, push) {
    $collapses.removeClass('show');
    $nonDefault.hide();
    $body.empty();

    $svg = null;
    document.body.style.overflow = '';

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
      graphView(path);
    }
    if (push) {
      window.history.pushState(null, null, '/debug/ot/' + path);
    }
  }

  function pathView() {
    view(location.pathname.substring('/debug/fs'.length).replace(/^\/|\/$/g, ''));
  }

  pathView();

  window.onpopstate = pathView;

  let grabbed = null;

  function transform(x, y, scale) {
    $graphCoords.text('x: ' + x.toFixed(0) + ', y: ' + y.toFixed(0) + ', scale: ' + scale.toFixed(3));
    $svg.data('pos', [x, y]);
    $svg.data('scale', scale);
    $svg.css('transform', 'translate(' + x + 'px, ' + y + 'px) scale(' + scale + ')');
  }

  function handleDrag(x, y, pressed) {
    if (grabbed == null) {
      if (pressed) {
        $diffModal.collapse('hide');
        let pos = $svg.data('pos');
        if (pos) {
          grabbed = [x - pos[0], y - pos[1]]
        } else {
          grabbed = [x, y];
        }
      }
    } else if ($svg != null) {
      let offX = x - grabbed[0];
      let offY = y - grabbed[1];
      let scale = $svg.data('scale') || 1;
      if (pressed) {
        transform(offX, offY, scale);
      } else {
        grabbed = null;
      }
    }
  }

  function handleScale(x, y, factor) {
    if ($svg) {
      $diffModal.collapse('hide');

      let prevScale = $svg.data('scale') || 1;
      let scale = prevScale;

      if (factor < 0) {
        if (scale > 0.1) {
          scale /= 1.1;
        }
      } else if (scale < 10) {
        scale *= 1.1;
      }

      let box = $svg[0].getBoundingClientRect();

      let [offX, offY] = ($svg.data('pos') || [0, 0]);

      offX += (box.x - x) * (scale / prevScale - 1);
      offY += (box.y - y) * (scale / prevScale - 1);

      transform(offX, offY, scale);
    }
  }

  let pressed = false;
  $body.mousedown(() => pressed = true);
  $body.mousewheel(e => handleScale(e.pageX, e.pageY, e.deltaY));
  $(document).mouseup(e => handleDrag(e.pageX, e.pageY, pressed = false));
  $(document).mousemove(e => handleDrag(e.pageX, e.pageY, pressed));
};
