window.onload = () => {
  $('.reply').click(e => {
    let exact = $('#reply_' + e.target.dataset.postId);
    let was = exact.hasClass('d-none');
    $('.reply-form').addClass('d-none');
    if (was) {
      exact.removeClass('d-none')
    } else {
      exact.addClass('d-none')
    }
  });
  $('.delete').click(e => {
    if (confirm('are you sure you want to delete post?')) {
      alert('TODO');
    }
  });

  $(window).on('unload', () => localStorage.setItem('scroll', $(document).scrollTop()));
  let scroll = localStorage.getItem('scroll');
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }
};
