window.onload = () => {
  $('.custom-file input').change(e => $(this).next('.custom-file-label').html(Array.prototype.slice.call(e.target.files).map(f => f.name).join(', ')));

  $('[data-post-button]').keydown((e) => {
    if (e.keyCode === 13 && e.ctrlKey) {
      e.preventDefault();
      $(`#${e.target.dataset.postButton}`).click();
    }
  });

  let $loginButton = $('#login_button');
  $loginButton.attr('href', $loginButton.attr('href') + window.location.href);

  $(window).on('beforeunload', () => localStorage.setItem('scroll', $(document).scrollTop()));
  let scroll = localStorage.getItem('scroll');
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }
};
