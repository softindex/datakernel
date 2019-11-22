window.onload = () => {
  // region * attachment utils
  function addAttachments(formData, $addAttachments, postId) {
    function addAll(formData, fieldName, $input) {
      let files = $input[0].files;
      for (let i = 0; i < files.length; i++) {
        let f = files[i];
        formData.append(fieldName, f, f.name);
      }
      $input.val('');
      $input.trigger('change');
    }

    let $imageAttachment = $addAttachments.find('#image_attachment_' + postId);
    let $videoAttachment = $addAttachments.find('#video_attachment_' + postId);
    let $documentAttachment = $addAttachments.find('#document_attachment_' + postId);

    addAll(formData, 'image_attachment', $imageAttachment);
    addAll(formData, 'video_attachment', $videoAttachment);
    addAll(formData, 'document_attachment', $documentAttachment);

    return () => {
      $imageAttachment.val('');
      $videoAttachment.val('');
      $documentAttachment.val('');
    };
  }

  // endregion

  // region * textarea utils
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
    let $stub = $('<div style="height: ' + $textarea.height() + 'px"></div>');
    $textarea.after($stub);
    $textarea.css('height', '1px');
    $textarea.css('height', (2 + $textarea[0].scrollHeight) + 'px');
    $stub.remove();
  }
  // endregion

  // region * modals setup
  let $imageModal = $('#image-modal');
  let $imageModalImg = $imageModal.find('img');

  let $videoModal = $('#video-modal');
  let $videoModalVid = $videoModal.find('video');

  $imageModal.click(() => {
    $imageModal.addClass('d-none');
    document.body.style.overflow = 'auto';
  });

  $videoModal.click(() => {
    $videoModal.addClass('d-none');
    document.body.style.overflow = 'auto';
  });

  function checkImageModalOverflow() {
    if ($imageModalImg.height() > $(window).height()) {
      $imageModal.addClass('overflow');
    } else {
      $imageModal.removeClass('overflow');
    }
  }

  $(window).resize(() => checkImageModalOverflow());

  // endregion

  let shownReply = null;

  function addPostCallbacks($element) {
    // region * handle editing
    $element.find('.edit-button').click(e => {
      let threadId = e.target.dataset.threadId;
      let postId = e.target.dataset.postId;

      let $content = $('#content_' + postId);

      let $save = $('<button class="btn btn-sm btn-primary">save</button>');
      let $attach = $('<button class="btn btn-sm btn-outline-primary mr-1">attach</button>');
      let $cancel = $('<button class="btn btn-sm btn-outline-primary mr-1">cancel</button>');
      let $textarea = $('<textarea class="form-control m-0 p-2" style="resize: none"></textarea>');
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
      row.append($attach);
      row.append($cancel);
      row.append($save);

      let $shownAttachments = $element.find('#attachments_' + postId);
      let $editAttachments = $element.find('#edit_attachments_' + postId);
      let $addAttachments = $element.find('#edit_add_attachments_' + postId);

      $attach.click(() => $addAttachments.collapse('toggle'));

      $shownAttachments.addClass('d-none');
      $editAttachments.removeClass('d-none');

      $save.click(() => {
        if (!validate($textarea, doValidate, true)) {
          return;
        }
        let formData = new FormData();
        let content = $textarea.val().trim();
        formData.append('content', content);

        let clearAttachments = addAttachments(formData, $addAttachments, postId);

        let removedAttachments = [];
        $element.find('#edit_attachments_' + postId + ' .cross')
          .each((_, e) => {
            if (e.checked) {
              removedAttachments.push($(e).prev().text());
            }
          });
        formData.append('removeAttachments', removedAttachments.join(','));

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
            $addAttachments.removeClass('show');

            clearAttachments();

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
        $attach.remove();
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
    $element.find('.reply-button').click(e => {
      let postId = $(e.target).data('postId');
      let threadId = $(e.target).data('threadId');

      // closing the opened reply
      if (shownReply !== null) {
        let captured = shownReply;
        shownReply.on('hidden.bs.collapse', () => captured.remove());
        shownReply.collapse('hide');

        // if it's the same reply then we do nothing
        if (shownReply.data('postId') === postId) {
          shownReply = null;
          return;
        }
      }

      let $reply =
        // region reply html
        $('<div class="collapse" data-post-id="' + postId + '">' +
            '<div class="pt-2">' +
              '<textarea class="form-control form-control-sm reply-content pr-5"' +
              ' placeholder="Post your reply"' +
              ' name="content"' +
              ' style="resize: none; height: 31px"></textarea>' +
              '<div id="add_attachments_' + postId + '" class="collapse">' +
                '<div class="mt-2">' +
                  '<div class="custom-file col-3 mr-1">' +
                    '<input type="file" accept="image/*" class="custom-file-input"' +
                    ' id="image_attachment_' + postId + '" multiple' +
                    ' name="image_attachment">' +
                    '<label class="custom-file-label text-truncate" for="image_attachment_' + postId + '">' +
                    ' Attach images' +
                    '</label>' +
                  '</div>' +
                  '<div class="custom-file col-3 mr-1">' +
                    '<input type="file" accept="video/mp4" class="custom-file-input"' +
                    ' id="video_attachment_' + postId + '" multiple' +
                    ' name="video_attachment">' +
                    '<label class="custom-file-label text-truncate" for="video_attachment_' + postId + '">' +
                    'Attach videos' +
                    '</label>' +
                  '</div>' +
                  '<div class="custom-file col-3">' +
                    '<input type="file" class="custom-file-input"' +
                    ' id="document_attachment_' + postId + '" multiple' +
                    ' name="document_attachment">' +
                    '<label class="custom-file-label text-truncate" for="document_attachment_' + postId + '">' +
                    'Attach documents' +
                    '</label>' +
                  '</div>' +
                '</div>' +
              '</div>' +
              '<div class="my-2">' +
                '<button class="btn btn-sm btn-secondary post-button">post</button>' +
                '<button class="ml-1 btn btn-sm btn-outline-secondary"' +
                ' data-toggle="collapse"' +
                ' data-target="#add_attachments_' + postId + '">attach</button>' +
              '</div>' +
            '</div>' +
          '</div>');
      // endregion

      handleAttachments($reply);

      let $textarea = $reply.find('textarea');
      let $post = $reply.find('.post-button');

      // region * auto-post when pressing ctrl+enter
      $textarea.keydown(e => {
        if (e.keyCode === 13 && e.ctrlKey) {
          $reply.find('.post-button').click();
        }
      });
      // endregion

      let doValidate = {it: false};
      $textarea.on('input', () => {
        autoresize($textarea);
        validate($textarea, doValidate);
      });

      $post.click(() => {
        if (!validate($textarea, doValidate, true)) {
          return;
        }

        let formData = new FormData();
        formData.append('content', $textarea.val().trim());

        let $addAttachments = $reply.find('#add_attachments_' + postId);

        let clearAttachments = addAttachments(formData, $addAttachments, postId);

        fetch('/' + threadId + '/' + postId, {method: 'POST', body: formData})
          .then(r => {
            if (r.ok) {
              return r.text();
            }
            throw new Error('failed posting reply');
          })
          .then(text => {
            $textarea.val('');
            autoresize($textarea);
            $reply.removeClass('show');
            $addAttachments.removeClass('show');
            clearAttachments();

            doValidate.it = false;

            let $rerendered = $(text);
            if ($rerendered.children().first().next().children().first().data('inlineParent')) {
              $reply.parent().parent().parent().parent().append($rerendered);
            } else {
              $reply.parent().append($rerendered);
            }
            handleReferences($rerendered);
            addPostCallbacks($rerendered);
          }, console.error);
      });
      // textarea focus when you press the reply button
      $reply.on('show.bs.collapse', () => setTimeout(() => $textarea.focus(), 0));

      shownReply = $reply;
      $('#post_' + postId).next().prepend($reply);
      $reply.collapse('show');
    });
    // endregion

    // region * handle deleting and restoring
    $element.find('[data-post-call]').click(e => {
      let element = $('#post_' + e.target.dataset.postId);

      fetch(e.target.dataset.postCall, {method: 'POST'})
        .then(r => r.text())
        .then(text => {
          let $rerendered = $(text);
          addPostCallbacks($rerendered);
          element.replaceWith($rerendered);
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

    // region * opening image modal
    $element.find('[data-image-modal]').click(e => {
      if (e.target.tagName !== 'A') {
        $imageModalImg[0].src = e.target.dataset.imageModal;

        function openModal() {
          $imageModal.removeClass('d-none');
          checkImageModalOverflow();
          document.body.style.overflow = 'hidden';
        }

        setTimeout(() => {
          if ($imageModalImg[0].loaded) {
            openModal();
          } else {
            $imageModalImg[0].onload = openModal;
          }
        }, 0);
      }
    });
    // endregion

    // region * opening video modal
    $element.find('[data-video-modal]').click(e => {
      if (e.target.tagName !== 'A') {
        $videoModalVid[0].src = e.target.dataset.videoModal;
        $videoModal.removeClass('d-none');
        document.body.style.overflow = 'hidden';
      }
    });
    // endregion
  }

  addPostCallbacks($(document));

  // region * handle login button
  let $loginButton = $('#login_button');
  $loginButton.attr('href', $loginButton.attr('href') + '?redirectURI=' + encodeURIComponent(location.origin) + '/authorize%3Forigin=' + encodeURIComponent(location.href));
  // endregion

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

    let renamed = $input.val().trim();
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
  const scrollAnimationTime = 400;

  function handleReferences($element) {
    $element.find('[data-post-reference]').on('click', e => {
      let post = $(e.target.dataset.postReference);
      let animated = scrollTo(post);
      setTimeout(() => {
        post.removeClass('target-fade').addClass('target-fade');
        setTimeout(() => post.removeClass('target-fade'), 700);
      }, animated ? scrollAnimationTime : 0);
    });

    function scrollTo($target) {
      let windowHeight = window.innerHeight || document.documentElement.clientHeight;
      let top = $target.offset().top;
      if (top < $(window).scrollTop()) {
        $('html, body').animate({scrollTop: top - 10}, scrollAnimationTime);
        return true;
      } else if (top + $target.height() > $(window).scrollTop() + windowHeight) {
        $('html, body').animate({scrollTop: top - windowHeight + $target.height() + 10}, scrollAnimationTime);
        return true;
      }
    }
  }
  handleReferences($(document));
  // endregion

  // region * handle filenames in attachments
  function handleAttachments($element) {
    $element.find('input[type=file]').change(e => {
      const input = e.target;
      const files = input.files;
      const type = input.name.split('_')[0];
      const numberOfFiles = files.length;
      const label = $(input).next('label');

      if (numberOfFiles > 1) {
        label.html('[' + numberOfFiles + ' ' + type + 's]')
      } else if (numberOfFiles === 1) {
        label.html(files[0].name.split('\\').pop());
      } else if (numberOfFiles === 0) {
        label.html('Attach ' + type + 's');
      }
    });
  }
  handleAttachments($(document));
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
