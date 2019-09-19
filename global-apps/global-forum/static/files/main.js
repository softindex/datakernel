window.onload = () => {

  $('[data-edit]').click((e) => {
    let element = $(e.target.dataset.edit);
    let lines = Math.round(element.height() / parseFloat(element.css('line-height')));

    let save = $(`<button class="btn btn-sm btn-primary">save</button>`);
    let cancel = $(`<button class="btn btn-sm btn-outline-primary mx-1">cancel</button>`);
    let textarea = $(`<textarea class="form-control" rows="${lines}"></textarea>`);

    textarea.keydown((e) => {
      if (e.keyCode === 13 && e.ctrlKey) {
        e.preventDefault();
        save.click();
      } else if (e.keyCode === 27) {
        cancel.click();
      }
    });
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

  // make pressing ctrl+enter submit the current post
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

    if (numberOfFiles > 1){
      label.html(`[${numberOfFiles} ${type}s]`)
    } else if (numberOfFiles === 1){
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
    document.execCommand("copy");
    setTimeout(() => target.popover('hide'), 1000);
    e.preventDefault();
    return false;
  });

  // handle textarea focus when reply button is pressed
  $('.collapse').on('show.bs.collapse', e => {
    let textarea = $(e.target).find('textarea');
    if (textarea.length) {
      setTimeout(() => textarea.focus(), 0);
    }
  });
};
