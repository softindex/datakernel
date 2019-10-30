window.onload = () => {

  function addAll(formData, fieldName, $input) {
    let files = $input[0].files;
    for (let i = 0; i < files.length; i++) {
      let f = files[i];
      formData.append(fieldName, f, f.name);
    }
    $input.val('');
    $input.trigger('change');
  }

  function validate($textarea, doValidate, set) {
    if (set) {
      doValidate.it = true;
    } else if (!doValidate.it) {
      return true;
    }
    let content = $textarea.val().trim();
    if (content.length === 0 || content.length > 4000) {
      $textarea.removeClass('is-valid').addClass('is-invalid');
      return false;
    } else {
      if (!set) {
        $textarea.addClass('is-valid');
      }
      $textarea.removeClass('is-invalid');
      return true;
    }
  }

  function autoresize($textarea) {
    $textarea.css('height', '1px');
    $textarea.css('height', (2 + $textarea[0].scrollHeight) + 'px');
  }

  function addPostCallbacks($element) {
    // region * handle editing
    $('.edit-button').click(e => {
      let threadId = e.target.dataset.threadId;
      let postId = e.target.dataset.postId;

      let $content = $('#content_' + postId);

      let $save = $('<button class="btn btn-sm btn-primary">save</button>');
      let $cancel = $('<button class="btn btn-sm btn-outline-primary mx-1">cancel</button>');
      let $textarea = $('<textarea class="form-control m-0 p-2 mb-3"></textarea>');
      let doValidate = {it: false};

      setTimeout(() => autoresize($textarea), 0);

      $textarea.on('input', () => {
        autoresize($textarea);
        validate($textarea, doValidate);
      });

      // 'ctrl+enter' => save the edit and 'escape' => cancel editing
      $textarea.keydown(e => {
        if (e.keyCode === 13 && e.ctrlKey) {
          e.preventDefault();
          $save.click();
        } else if (e.keyCode === 27) {
          $cancel.click();
        }
      });

      let dropdown = $(e.target.parentElement).removeClass('show');
      let row = dropdown.parent().parent();
      row.children().addClass('d-none');
      row.append($cancel);
      row.append($save);

      let $shownAttachments = $('#attachments_' + postId);
      let $editAttachments = $('#edit_attachments_' + postId);

      $shownAttachments.addClass('d-none');
      $editAttachments.removeClass('d-none');

      $save.click(() => {
        if (!validate($textarea, doValidate, true)) {
          return;
        }
        let formData = new FormData();
        let content = $textarea.val().trim();
        formData.append('content', content);
        fetch('/' + threadId + '/' + postId + '/edit', {method: 'POST', body: formData})
          .then(r => {
            if (r.ok) {
              return r.text();
            }
            throw new Error('failed to edit')
          })
          .then(text => {
            doValidate.it = false;
            $shownAttachments.removeClass('d-none');
            $editAttachments.addClass('d-none');

            let rerendered = $(text);
            $('#post_' + postId).replaceWith(rerendered);
            addPostCallbacks(rerendered);
          }, e => {
            console.error(e);
            $cancel.click();
          });
      });

      $content.addClass('d-none').after($textarea);
      $textarea.focus().val($content.text());

      $cancel.click(() => {
        $(document).focus();
        $textarea.remove();
        $cancel.remove();
        $save.remove();
        $content.removeClass('d-none');
        row.children().removeClass('d-none');
        $shownAttachments.removeClass('d-none');
        $editAttachments.addClass('d-none');
      });
    });

    // endregion

    // region * handle replies
    $element.find('.post-button').click(e => {
      let threadId = e.target.dataset.threadId;
      let postId = e.target.dataset.postId;

      let $textarea = $('#reply_content_' + postId);
      let doValidate = $textarea.data('doValidate');
      if (!validate($textarea, doValidate, true)) {
        return;
      }

      let formData = new FormData();
      formData.append('content', $textarea.val().trim());

      addAll(formData, 'image_attachment', $('#image_attachment_' + postId));
      addAll(formData, 'video_attachment', $('#video_attachment_' + postId));
      addAll(formData, 'document_attachment', $('#document_attachment_' + postId));

      fetch('/' + threadId + '/' + postId, {method: 'POST', body: formData})
        .then(r => {
          if (r.ok) {
            return r.text();
          }
          throw new Error('failed posting reply');
        })
        .then(text => {
          let $reply = $('#reply_' + postId);
          $textarea.val('');
          $reply.removeClass('show');
          $('#add_attachments_' + postId).removeClass('show');
          doValidate.it = false;

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
    // textarea focus when reply button is pressed
    $element.find('.collapse').on('show.bs.collapse', e => {
      let textarea = $(e.target).find('textarea');
      setTimeout(() => textarea.focus(), 0);
    });
    // textarea validation and resizing, just like when editing
    $element.find('.reply-content').each((_, elem) => {
      let $textarea = $(elem);
      let doValidate = {it: false};
      $textarea.data('doValidate', doValidate);
      $textarea.css('height', 'calc(1.5em + .75rem + 2px)');
      $textarea.on('input', () => {
        autoresize($textarea);
        validate($textarea, doValidate);
      });
    });

    // endregion

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

  $title.dblclick(() => $edit.click());

  $edit.click(() => {
    $title.addClass('d-none');
    $input.val($title.text().trim());
    $input.removeClass('d-none');
    $input.focus();
    $input.select();

    $edit.addClass('d-none');
    $cancel.removeClass('d-none');

    $delete.addClass('d-none');
    $save.removeClass('d-none');
  });

  // 'ctrl+enter' => save the edit and 'escape' => cancel editing
  $input.keydown((e) => {
    if (e.keyCode === 13 && e.ctrlKey) {
      e.preventDefault();
      $save.click();
    } else if (e.keyCode === 27) {
      $cancel.click();
    }
  });

  $cancel.click(() => {
    $title.removeClass('d-none');
    $input.addClass('d-none');
    inputForm.removeClass('was-validated');

    $edit.removeClass('d-none');
    $cancel.addClass('d-none');

    $delete.removeClass('d-none');
    $save.addClass('d-none');
  });

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
      .then(() => $cancel.click(), console.error);
  });

  $deleteConfirm.click(() => {
    let threadId = $deleteConfirm.data('threadId');

    fetch('/' + threadId, {method: 'DELETE'})
      .then(() => location.href = '/', console.error);
  });
  // endregion

  // region * auto-submit forms when pressing ctrl+enter
  $('[data-post-button]').keydown(e => {
    if (e.keyCode === 13) {
      if (e.ctrlKey) {
        $(e.target.dataset.postButton).click();
      }
      if (e.target.tagName !== 'TEXTAREA') {
        console.log(e.target.tagName);
        e.preventDefault();
      }
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

  // region * imitating anchor-links without mandatory scroll-jumps and with yellow-fade
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
  $('.custom-file-input').change(e => {
    const input = e.target;
    const files = input.files;
    const type = input.name.split('_')[0];
    const numberOfFiles = files.length;
    const label = $(input).next('.custom-file-label');

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

  // region * configure the datetime picker
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
