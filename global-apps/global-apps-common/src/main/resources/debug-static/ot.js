window.onload = () => {
  let $title = $('.title');
  let $key = $('.key');
  let $body = $('.body');
  let $diffModal = $('.diff-modal');
  let $graphCoords = $('.graph-coords');

  let $svg = null;

  let graphTimerId = null;

  $diffModal.find('.diff-hide').click(() => $diffModal.collapse('hide'));

  let viz = new Viz();

  function updateGraph(pk, repo) {
    fetch('/debug/ot/api/' + pk + '/' + repo)
      .then(r => r.text())
      .then(text => {
        let s = text.replace(/label=/g, 'xlabel=');//.replace(/label="([^"]*?)\|([^"]*?)"/g, 'label="$1"');
        return viz.renderSVGElement(s);
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
          let diff = text.endsWith(',') ? text.substring(0, text.length - 1) : text;
          let edge = $e.parent().attr('id');
          let diffs = diffMap[edge];
          if (!diffs) {
            diffs = [];
            diffMap[edge] = diffs;
          }
          diffs.push(diff.split('|'));
          // diffs.push([diff, diff]);
          // $e.attr('font-family', 'sans-serif').attr('font-size', '10');
          $e.remove();
        });

        $svg.find('g.graph > title').remove(); // remove the %0 global title (idk what is this)
        $svg.find('g.graph > polygon').remove(); // remove white background

        let $graph = $svg.find('g.graph');
        $graph.attr('transform', null);

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
            .append(diffMap[id].map(diff => $('<div class="diff"></div>').html(diff[1].replace(/UserId{pl='([^']*)'}/g, '<div class="user-id" title="$1">USER</div>'))));

          let rect = e.currentTarget.getBoundingClientRect();
          $diffModal.css('left', rect.x - $diffModal.width() / 2);
          $diffModal.css('top', rect.y + rect.height / 2 + $(window).scrollTop());
          $diffModal.collapse('show');
        });

        $body.empty().append($('<div class="mx-auto"></div>').append($svg));

        let bbox = $svg[0].getBBox();
        $svg.attr('width', bbox.width);
        $svg.attr('height', bbox.height);
        $svg.attr('viewBox', [bbox.x - 10, bbox.y - 10, bbox.width + 10, bbox.height + 10].join(' '));

        transform(pos[0], pos[1], scale)
      }, e => {
        viz = new Viz();
        console.error(e);
      })
  }

  function graphView(pk, repo) {
    $title.text('Commit graph for a repo:');
    $key.text(repo);
    $graphCoords.show();
    document.body.style.overflow = 'hidden';
    updateGraph(pk, repo);

    function graphTimer() {
      graphTimerId = setTimeout(() => {
        updateGraph(pk, repo);
        graphTimer();
      }, 1000);
    }

    // graphTimer();
  }

  function listView(pk) {
    function update() {
      $title.text('OT repos of:');
      $key.text(pk);

      createTreeList('/debug/ot/api/' + pk, repo => view(pk + '/' + repo))
        .then($list => $body.append($('<div class="container"></div>').append($list)), console.log);
    }

    update();
  }

  function view(path, push = true) {
    let [pk, ...tail] = path.split('/');

    $diffModal.removeClass('show');
    $graphCoords.hide();
    $svg = null;
    $body.empty();
    document.body.style.overflow = 'initial';
    if (graphTimerId) {
      clearTimeout(graphTimerId);
    }

    if (tail.length === 0) {
      listView(pk);
    } else {
      graphView(pk, tail.join('/'));
    }
    if (push) {
      window.history.pushState(null, null, '/debug/ot/' + path);
    }
  }

  function pathView() {
    let path = location.pathname;
    view(path.substring('/debug/ot/'.length, path.endsWith('/') ? path.length - 1 : undefined), false);
  }

  pathView();

  window.onpopstate = pathView;

  let grabbed = null;

  function transform(x, y, scale) {
    $svg.data('pos', [x, y]);
    $svg.data('scale', scale);
    $svg.css('transform', 'translate(' + x + 'px, ' + y + 'px) scale(' + scale + ')');
    $graphCoords.text('x: ' + x.toFixed(0) + ', y: ' + y.toFixed(0) + ', scale: ' + scale.toFixed(3));
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
  $(document).mousedown(() => pressed = true);
  $(document).mouseup(e => handleDrag(e.pageX, e.pageY, pressed = false));
  $(document).mousemove(e => handleDrag(e.pageX, e.pageY, pressed));
  $(document).mousewheel(e => handleScale(e.pageX, e.pageY, e.deltaY));
};
