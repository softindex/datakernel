window.onload = () => {
  $('.delete').click(e => {
    fetch(e.target.dataset.url, {method: 'delete'}).then(() => location.reload(), console.error);
  });
  $('.restore').click(e => {
    fetch(e.target.dataset.url + '/restore', {method: 'post'}).then(() => location.reload(), console.error);
  });
  $('.custom-file input').change(function (e) {
    $(this).next('.custom-file-label').html(Array.prototype.slice.call(e.target.files).map(f => f.name).join(', '));
  });


  $(window).on('beforeunload', () => localStorage.setItem('scroll', $(document).scrollTop()));
  let scroll = localStorage.getItem('scroll');
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }
};
