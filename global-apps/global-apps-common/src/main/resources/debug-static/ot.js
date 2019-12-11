window.onload = () => {
  let $title = $('#title');
  let $body = $('#body');
  let $polling = $('#polling');
  let $pollingCb = $polling.find('input:first');

  let $collapses = $('.collapse');
  let $nonDefault = $('.non-default');

  let $svg = null;
  let pollTimerId = null;
  let listTimerId = null;

  init();

  let viz = new Viz();

  function updateGraph(repo) {
    fetch('/debug/ot/api/' + repo)
      .then(r => {
        if (!r.ok) {
          throw new Error('failed to load the graph');
        }
        return r.text();
      })
      .then(text => {
        let replaced = text.replace(/label="([^]*?)"/gm, (_, b64) => {
          let diffs = b64.split(',\n').map(encoded => JSON.parse(atob(encoded)));
          let shortLabels = diffs.map(d => d[0].replace(/\n/g, '\n+').replace(/"/g, '\\"'));
          shortLabels.reverse();
          let labels = '+' + shortLabels.join('\n+');
          let xlabels = diffs.map(d => d[1].replace(/"/g, '\\"')).join('\n');
          return 'label="' + labels + '"; xlabel="' + xlabels + '"';
        });
        return viz.renderSVGElement(replaced);
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
          diffs.unshift(text);
          $e.remove();
        });

        $svg.find('g.graph > title').remove(); // remove the %0 global title (idk what is this)
        $svg.find('g.graph > polygon').remove(); // remove white background

        let $graph = $svg.find('g.graph');
        $svg.addClass('unselectable');
        $graph.attr('transform', 'scale(1.5)');

        $body.empty().append($('<div class="mx-auto mt-5"></div>').append($svg));

        // set the size of svg to match its fixed contents (no big white background rect)
        let bbox = $svg[0].getBBox();
        $svg.attr('width', bbox.width + 2);
        $svg.attr('height', bbox.height + 2);
        $svg.attr('viewBox', [bbox.x - 2, bbox.y - 2, bbox.width + 4, bbox.height + 4].join(' '));

        let $edges = $svg.find('g.edge');

        // create and show a diff modal on edge click
        $edges.click(e => {
          if (grabbed) { // prevent this when releasing mouse after drag finish
            return;
          }
          let edgeId = e.currentTarget.id;
          let $prev = $('[data-edge="' + edgeId + '"]');
          if ($prev.length > 0) {
            $prev.collapse('hide');
            return;
          }

          let title = 'Diffs for: ' + $(e.currentTarget).find('title').text();
          let rect = e.currentTarget.getBoundingClientRect();
          let $diffModal = createDiffModal(title, edgeId, diffMap[edgeId]);

          $(document.body).append($diffModal);

          $diffModal
            .css('left', rect.x - $diffModal.width() / 2)
            .css('top', rect.y + rect.height / 2 + $(window).scrollTop())
            .collapse('show');
        });

        // add transparent rects to each edge group for them to be clickable
        $edges.each((_, e) => {
          let bbox = e.getBBox();
          // jquery can't properly create svg elements
          let $rect = $(document.createElementNS('http://www.w3.org/2000/svg', 'rect'))
            .attr('x', bbox.x)
            .attr('y', bbox.y)
            .attr('width', bbox.width)
            .attr('height', bbox.height)
            .attr('opacity', '0');
          $(e).prepend($rect);
        });

        transform(pos[0], pos[1], scale);
      }, e => {
        viz = new Viz();
        console.error(e);
      })
  }

  function graphView(repo) {
    $title.text('Commit graph of repository \'' + repo + '\'');
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
    $('.diff-modal').remove();
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
  let grabbedModals = null;
  let $movingModal = false;

  function transform(x, y, scale) {
    $svg.data('pos', [x, y]);
    $svg.data('scale', scale);
    $svg.css('transform', 'translate(' + x + 'px, ' + y + 'px) scale(' + scale + ')');
  }

  function handleDrag(x, y, pressed) {
    if (grabbed == null) {
      if (pressed) {
        if ($movingModal) {
          $movingModal.addClass('unselectable');
          grabbedModals = [x - parseFloat($movingModal.css('left')), y - parseFloat($movingModal.css('top'))];
        } else {
          let $diffModals = $('.diff-modal');
          $diffModals.addClass('unselectable');
          grabbedModals = $diffModals.toArray().map(e => {
            let $e = $(e);
            return [$e, x - parseFloat($e.css('left')), y - parseFloat($e.css('top'))];
          });
        }
        let pos = $svg.data('pos');
        if (pos) {
          grabbed = [x - pos[0], y - pos[1]]
        } else {
          grabbed = [x, y];
        }
      }
    } else if ($movingModal) {
      if (pressed) {
        $movingModal.css('left', x - grabbedModals[0]).css('top', y - grabbedModals[1]);
      } else {
        $movingModal.removeClass('unselectable');
        $movingModal = null;
        grabbedModals = null;
        grabbed = null;
      }
    } else if ($svg != null) {
      let scale = $svg.data('scale') || 1;
      if (pressed) {
        let offX = x - grabbed[0];
        let offY = y - grabbed[1];
        transform(offX, offY, scale);
        grabbedModals.forEach(([$e, gx, gy]) => $e.css('left', x - gx).css('top', y - gy));
      } else {
        $('.diff-modal').removeClass('unselectable');
        grabbedModals = null;
        setTimeout(() => grabbed = null, 1); // for the modal-after-drag prevention
      }
    }
  }

  function handleScale(x, y, factor) {
    if ($svg) {
      $('.diff-modal').collapse('hide');

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

  function createDiffModal(title, edge, diffs) {
    let $diffModal = $('<div class="diff-modal collapse" data-edge="' + edge + '">' +
      '<div class="diff-title unselectable">' +
      '<span class="diff-title-content">' + title + '</span>' +
      '<span class="float-right px-2 py-0 m-1 diff-hide">&times;</span>' +
      '</div>' +
      '<div class="diff-body"></div>' +
      '</div>');
    $diffModal.on('hidden.bs.collapse', e => $(e.currentTarget).remove());
    $diffModal.find('.diff-title').mousedown(e => {
      if (e.target.classList.contains('diff-hide') || e.button === 1) {
        $diffModal.collapse('hide');
        return;
      }
      pressed = true;
      $movingModal = $(e.currentTarget).parent();
      $movingModal.appendTo($movingModal.parent());
    });
    $diffModal.find('.diff-body').append(diffs.map(diff => $('<div class="diff"></div>').html(diff)));
    updateTimestamps($diffModal);
    return $diffModal
  }

  $body.mousedown(() => pressed = true);
  $body.mousewheel(e => handleScale(e.pageX, e.pageY, e.deltaY));
  $(document).mouseup(e => handleDrag(e.pageX, e.pageY, pressed = false));
  $(document).mousemove(e => handleDrag(e.pageX, e.pageY, pressed));
};
