
window.onload = () => {

  // handle editing
  $('[data-edit]').click((e) => {
    let element = $(e.target.dataset.edit);
    let lines = Math.round(element.height() / parseFloat(element.css('line-height')));

    let save = $(`<button class="btn btn-sm btn-primary">save</button>`);
    let cancel = $(`<button class="btn btn-sm btn-outline-primary mx-1">cancel</button>`);
    let textarea = $(`<textarea class="form-control" rows="${lines}"></textarea>`);

    // 'ctrl+enter' => save the edit and 'escape' => cancel editing
    textarea.keydown((e) => {
      if (e.keyCode === 13 && e.ctrlKey) {
        e.preventDefault();
        save.click();
      } else if (e.keyCode === 27) {
        cancel.click();
      }
    });

    // change text-area height on typing
    textarea.on('input', () => {
      let lines = textarea.val().split('\n').length;
      textarea.attr('rows', lines);
    });

    let dropdown = $(e.target.parentElement).removeClass('show');
    let row = dropdown.parent().parent();
    row.children().addClass('d-none');
    row.append(cancel);
    row.append(save);

    save.click(() => {
      let formData = new FormData();
      let content = textarea.val();
      formData.append('content', content);
      fetch(e.target.dataset.editAction, {
        method: 'POST',
        body: formData
      }).then(() => location.reload(), e => {
        console.error(e);
        cancel.click();
      });
    });

    element.addClass('d-none').after(textarea);
    textarea.focus().val(element.text());

    cancel.click(() => {
      $(document).focus();
      textarea.remove();
      cancel.remove();
      save.remove();
      element.removeClass('d-none');
      row.children().removeClass('d-none');
    });
  });

  // auto-submit form when pressing ctrl+enter
  $('[data-post-button]').keydown((e) => {
    if (e.keyCode === 13 && e.ctrlKey) {
      e.preventDefault();
      $(e.target.dataset.postButton).click();
    }
  });

  // store scroll position
  $(window).on('beforeunload', () => localStorage.setItem('scroll', $(document).scrollTop()));
  let scroll = localStorage.getItem('scroll');
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }

  // handle filenames in attachments
  $('.custom-file input').change(() => {
    const thisElement = $(this)[0];
    const files = thisElement.files;
    const type = thisElement.name.split('_')[0];
    const numberOfFiles = files.length;
    const label = $(this).next('.custom-file-label');

    if (numberOfFiles > 1) {
      label.html(`[${numberOfFiles} ${type}s]`)
    } else if (numberOfFiles === 1) {
      label.html(files[0].name.split('\\').pop());
    } else if (numberOfFiles === 0) {
      label.html(`Attach ${type}s`);
    }
  });

  // handle copy-on-click functionality
  let autocopied = $('.autocopy');
  autocopied.popover({
    content: 'Copied to clipboard',
    placement: 'left',
  });
  autocopied.click(e => {
    let target = $(e.target);
    e.target.select();
    e.target.setSelectionRange(0, 99999); /* For mobile devices */
    document.execCommand('copy');
    setTimeout(() => target.popover('hide'), 1000);
    e.preventDefault();
    return false;
  });

  // handle textarea focus when reply button is pressed
  $('.collapse').on('show.bs.collapse', e => {
    let textarea = $(e.target).find('textarea');
    setTimeout(() => textarea.focus(), 0);
  });

  // configure datetimepicker
  $('.date').datetimepicker({
    format: 'HH:mm:ss/DD.MM.YYYY',
    useCurrent: false,
  });

  $('.like').click(e => {
    let parent = $(e.target).parent();
    let prefix = parent.data('prefix');
    let child = parent.find('.rating');
    if (parent.hasClass('liked')) {
      fetch(`${prefix}/rate/null`, {method: 'POST'})
        .then(() => {
          parent.removeClass('liked');
          child.text(parseInt(child.text()) - 1);
        }, console.error)
    } else {
      fetch(`${prefix}/rate/like`, {method: 'POST'})
        .then(() => {
          parent.addClass('liked');
          if (parent.hasClass('disliked')) {
            parent.removeClass('disliked');
            child.text(parseInt(child.text()) + 2);
          } else {
            child.text(parseInt(child.text()) + 1);
          }
        }, console.error)
    }
  });
  $('.dislike').click(e => {
    let parent = $(e.target).parent();
    let prefix = parent.data('prefix');
    let child = parent.find('.rating');
    if (parent.hasClass('disliked')) {
      fetch(`${prefix}/rate/null`, {method: 'POST'})
        .then(() => {
          parent.removeClass('disliked');
          child.text(parseInt(child.text()) + 1);
        }, console.error)
    } else {
      fetch(`${prefix}/rate/dislike`, {method: 'POST'})
        .then(() => {
          parent.addClass('disliked');
          if (parent.hasClass('liked')) {
            parent.removeClass('liked');
            child.text(parseInt(child.text()) - 2);
          } else {
            child.text(parseInt(child.text()) - 1);
          }
        }, console.error)
    }
  });

  $('.validate').submit(e => {
    let form = e.target;
    if (form.checkValidity() === false) {
      e.preventDefault();
      e.stopPropagation();
    }
    form.classList.add('was-validated');
  });
};
