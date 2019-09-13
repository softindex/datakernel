window.onload = () => {
  $('[data-post-button]').keydown((e) => {
    if (e.keyCode === 13 && e.ctrlKey) {
      e.preventDefault();
      $(e.target.dataset.postButton).click();
    }
  });
  $('[data-post-content]').click((e) => {
    let element = $(e.target.dataset.postContent);
    let lines = Math.round(element.height() / parseFloat(element.css('line-height')));
    element.replaceWith(`<textarea class="form-control" rows="${lines}">${element.text()}</textarea><button class="btn btn-sm btn-primary">save</button>`);
  });

  $(window).on('beforeunload', () => localStorage.setItem('scroll', $(document).scrollTop()));
  let scroll = localStorage.getItem('scroll');
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }

  // handle filenames in attachments
  $('.custom-file input').change(function (e) {
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
};
