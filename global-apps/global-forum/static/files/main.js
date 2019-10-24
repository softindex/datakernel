window.onload = () => {

  function addAll(formData, fieldName, $input) {
    let files = $input[0].files;
    for (let i = 0; i < files.length; i++) {
      let f = files[i];
      formData.append(fieldName, f, f.name);
    }
    $input.val('');
  }

  function addPostCallbacks($element) {
    // region * handle deleting and restoring
    $element.find('[data-post]').click(e => {
      let element = $(e.target.dataset.post);

      fetch(e.target.dataset.postReply, {method: 'POST'})
        .then(r => r.text())
        .then(text => {
          let $rerendered = $(text);
          element.replaceWith($rerendered);
          addPostCallbacks($rerendered);
        }, console.error);
    });
    // endregion

    // region * handle editing
    $('[data-edit]').click(e => {
      let element = $(e.target.dataset.edit);
      let lineHeight = parseFloat(element.css('line-height'));
      let lines = Math.round(element.height() / lineHeight);

      let save = $('<button class="btn btn-sm btn-primary">save</button>');
      let cancel = $('<button class="btn btn-sm btn-outline-primary mx-1">cancel</button>');
      let textarea = $('<textarea class="form-control m-0 p-2 mb-3" rows="' + lines + '"></textarea>');

      textarea.css('height', (lines + 1) * lineHeight - 6);

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
        textarea.css('height', (lines + 1) * lineHeight - 6);
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

        fetch(e.target.dataset.editAction, {method: 'POST', body: formData})
          .then(r => r.text())
          .then(text => {
            let rerendered = $(text);
            $(e.target.dataset.postId).replaceWith(rerendered);
            addPostCallbacks(rerendered);
          }, e => {
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
    // endregion

    // region * handle likes and dislikes
    $element.find('.like').click(e => {
      let parent = $(e.target).parent();
      let prefix = parent.data('prefix');
      let child = parent.find('.rating');
      if (parent.hasClass('liked')) {
        fetch(prefix + '/rate/null', {method: 'POST'})
          .then(() => {
            parent.removeClass('liked');
            child.text(parseInt(child.text()) - 1);
          }, console.error)
      } else {
        fetch(prefix + '/rate/like', {method: 'POST'})
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
    $element.find('.dislike').click(e => {
      let parent = $(e.target).parent();
      let prefix = parent.data('prefix');
      let child = parent.find('.rating');
      if (parent.hasClass('disliked')) {
        fetch(prefix + '/rate/null', {method: 'POST'})
          .then(() => {
            parent.removeClass('disliked');
            child.text(parseInt(child.text()) + 1);
          }, console.error)
      } else {
        fetch(prefix + '/rate/dislike', {method: 'POST'})
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
    // endregion

    // region * handle replies
    $element.find('.post-button').click(e => {
      let postId = e.target.dataset.postId;
      let threadId = e.target.dataset.threadId;

      let formData = new FormData();

      let $content = $('#reply_content_' + postId);
      formData.append('content', $content.val());

      let selector = '#image_attachment_' + postId;
      console.log(selector);
      console.log($(selector));
      addAll(formData, 'image_attachment', $(selector));
      addAll(formData, 'video_attachment', $('#video_attachment_' + postId));
      addAll(formData, 'document_attachment', $('#document_attachment_' + postId));

      let $reply = $('#reply_' + postId);
      fetch('/' + threadId + '/' + postId, {method: 'POST', body: formData})
        .then(r => r.text())
        .then(text => {
          $reply.removeClass('show');
          $('#attachments_' + postId).removeClass('show');

          let $rerendered = $(text);
          if ($rerendered.data('inlineParent')) {
            $reply.parent().parent().append($rerendered);
          } else {
            $reply.parent().append($rerendered);
          }
          handleReferences($rerendered);
          addPostCallbacks($rerendered);
        }, console.error);
    });
    // endregion
  }

  addPostCallbacks($(document));

  //region * handle thread editing and deleting
  let $title = $('#thread-title');
  let $input = $('#thread-title-input');
  let inputForm = $input.parent();

  let $edit = $('#edit-thread');
  let $cancel = $('#edit-thread-cancel');

  let $delete = $('#delete-thread');
  let $deleteConfirm = $('#delete-thread-confirm');
  let $save = $('#edit-thread-save');

  $edit.click(() => {
    $title.addClass('d-none');
    $input.val($title.text().trim());
    $input.removeClass('d-none');

    $edit.addClass('d-none');
    $cancel.removeClass('d-none');

    $delete.addClass('d-none');
    $save.removeClass('d-none');
  });

  function cancelEditingThread() {
    $title.removeClass('d-none');
    $input.addClass('d-none');
    inputForm.removeClass('was-validated');

    $edit.removeClass('d-none');
    $cancel.addClass('d-none');

    $delete.removeClass('d-none');
    $save.addClass('d-none');
  }

  $cancel.click(cancelEditingThread);

  $save.click(() => {
    let threadId = $save.data('threadId');

    let renamed = $input.val();
    if (renamed.length === 0 || renamed.length > 120) {
      inputForm.submit();
      return;
    }

    $title.text(renamed);

    let params = new URLSearchParams();
    params.append('title', renamed);
    fetch('/' + threadId + '/rename', {method: 'POST', body: params})
      .then(() => cancelEditingThread(), console.error);
  });

  $deleteConfirm.click(() => {
    let threadId = $deleteConfirm.data('threadId');

    fetch('/' + threadId, {method: 'DELETE'})
      .then(() => location.href = '/', console.error);
  });
  // endregion

  // region * auto-submit forms when pressing ctrl+enter
  $('[data-post-button]').keydown((e) => {
    if (e.keyCode === 13) {
      if (e.ctrlKey) {
        $(e.target.dataset.postButton).click();
      }
      e.preventDefault();
    }
  });
  // endregion

  // region * store scroll position
  $(window).on('beforeunload', () => localStorage.setItem('scroll', $(document).scrollTop()));
  let scroll = localStorage.getItem('scroll');
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }
  // endregion

  // region * imitating anchor-links without mandatory scroll-jumps and with yellowfade
  function handleReferences($element) {
    $element.find('[data-post-reference]').on('click', e => {
      let post = $(e.target.dataset.postReference);
      scrollTo(post);
      post.removeClass('target-fade').addClass('target-fade');
      setTimeout(() => post.removeClass('target-fade'), 500);
    });

    function scrollTo($target) {
      let windowHeight = window.innerHeight || document.documentElement.clientHeight;
      let top = $target.position().top;
      if (top < jQuery(window).scrollTop()) {
        $('html,body').scrollTop(top);
      } else if (top + $target.height() > $(window).scrollTop() + windowHeight) {
        $('html,body').scrollTop(top - windowHeight + $target.height());
      }
    }
  }

  handleReferences($(document));
  // endregion

  // region * handle filenames in attachments
  $('.custom-file input').change(() => {
    const thisElement = $(this)[0];
    const files = thisElement.files;
    const type = thisElement.name.split('_')[0];
    const numberOfFiles = files.length;
    const label = $(this).next('.custom-file-label');

    if (numberOfFiles > 1) {
      label.html('[' + numberOfFiles + ' ' + type + 's]')
    } else if (numberOfFiles === 1) {
      label.html(files[0].name.split('\\').pop());
    } else if (numberOfFiles === 0) {
      label.html('Attach ' + type + 's');
    }
  });
  // endregion

  // region * handle copy-on-click functionality
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
  // endregion

  // region * handle textarea focus when reply button is pressed
  $('.collapse').on('show.bs.collapse', e => {
    let textarea = $(e.target).find('textarea');
    setTimeout(() => textarea.focus(), 0);
  });
  // endregion

  // region * configure datetimepicker
  $('.date').datetimepicker({
    format: 'HH:mm:ss/DD.MM.YYYY',
    useCurrent: false,
  });
  // endregion

  // region * check validity on form submit
  $('.validate').submit(e => {
    let form = e.target;
    if (form.checkValidity() === false) {
      e.preventDefault();
      e.stopPropagation();
      form.classList.add('was-validated');
    }
  });
  // endregion
};
