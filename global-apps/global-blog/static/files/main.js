let shownReply = null;

window.onload = () => {
  $('.full-post').each((_, p) => addPostCallbacks($(p)));
  handleFiles($('.custom-file input'));

  replaceAppStoreUrl();
  enableHotKeySubmit();
  enableEditPost();
  enableStorScrollPosition();
  enableValidation();
  enableHandlingRootEdit();
  enableHandlingEdit();
  enableEditingTitleWithDbClick();
  enableCreationNewThread();
  enableHandlingPagination();
};

function enableCreationNewThread() {
  const createThread = $("#create_thread");
  var forms = document.getElementsByClassName('needs-validation');
  var validation = Array.prototype.filter.call(forms, function (form) {
    form.addEventListener('submit', function (event) {
      event.preventDefault();
      event.stopPropagation();
      if (form.checkValidity() === false) {
        form.classList.add('was-validated');
      } else {
        const forms = $(".needs-validation");

        let progressBar = $('#progress');
        const attachments = $('[id$="_attachment"]').get();
        const formData = new FormData();
        createThread.hide();
        const $contentNewThread = $("#contentNewThread");
        $contentNewThread.prop('disabled', true);
        $("#attachment_group").hide();
        const $titleNewThread = $("#titleNewThread");
        $titleNewThread.prop('disabled', true);

        formData.append("title", $titleNewThread[0].value);
        formData.append("content", $contentNewThread[0].value);
        attachments.forEach(attachment => {
          if (attachment.files.length) {
            Array.from(attachment.files).forEach(file => {
              formData.append(attachment.name, file)
            });
          }
        });

        progressBar.parent().closest('div').css("display", "");
        const xhr = new XMLHttpRequest();
        xhr.onload = (exc) => {
          if (xhr.readyState === 4) {
            if (xhr.status === 200) {
              location.href = "/"
            } else {
              console.error(exc);
            }
          }
        };
        xhr.upload.addEventListener('progress', (event) => {
          const progress = Math.round(event.loaded / event.total * 100);
          progressBar.css("width", progress + "%");
          progressBar.html(progress + "%");
        });
        progressBar.show();
        xhr.open("POST", $(createThread)[0].dataset.url, true);
        xhr.send(formData);
      }
    }, false);
  });
}

function enableEditingTitleWithDbClick() {
  $("#thread-title").dblclick((e) => {
    e.stopPropagation();
    const currentEle = $(e.currentTarget);
    updateVal(currentEle);

    function updateVal(currentEle) {
      let textarea = $(`<textarea maxlength="${e.target.dataset.maxlength ? e.target.dataset.maxlength : 120}" required class="form-control" ></textarea>`);
      textarea.val(currentEle[0].dataset.value);

      $(currentEle).replaceWith(textarea);
      resizeTextArea(currentEle);
      textarea.focus();
      textarea.focusout(function () {
        let object = {};
        object[currentEle[0].dataset.field] = textarea.val();
        fetch(e.target.dataset.editAction, {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          },
          method: 'POST',
          body: JSON.stringify(object)
        }).then(() => location.reload(), console.error);
      });
    }
  })
}

function enableHandlingEdit() {
  $('[data-edit]').click((e) => {
    let element = $(e.target.dataset.edit);
    let save = $(`<button class="btn btn-sm btn-link text-primary">save</button>`);
    let cancel = $(`<button class="btn btn-sm btn-link text-secondary mx-1">cancel</button>`);
    let textarea = $(`<textarea maxlength="${e.target.dataset.maxlength ? e.target.dataset.maxlength : 120}" required class="form-control" ></textarea>`);

    textarea.keydown((e) => {
      if (e.keyCode === 13 && e.ctrlKey) {
        e.preventDefault();
        save.click();
      }
      if (e.keyCode === 27) {
        cancel.click();
      }
    });
    save.click(() => {
      let object = {};
      object[element[0].dataset.field] = textarea.val();
      fetch(e.target.dataset.editAction, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        method: 'POST',
        body: JSON.stringify(object)
      }).then(() => location.reload(), console.error);
    });

    element.after(textarea);
    textarea.focus();
    textarea.val(element[0].dataset.value);
    resizeTextArea(textarea);
    element.addClass('d-none');
    textarea.on('input', () => {
      let lines = textarea.val().split('\n').length;
      textarea.attr('rows', lines);
    });

    let dropdown = $(e.target.parentElement);
    dropdown.removeClass('show');

    let row = dropdown.parent().parent();
    row.children().addClass('d-none');
    row.append(cancel);
    row.append(save);

    cancel.click(() => {
      textarea.remove();
      cancel.remove();
      save.remove();
      element.removeClass('d-none');
      row.children().removeClass('d-none');
    });
  })
}

function enableEditPost() {
  let $title = $('#thread-title');
  let $input = $('#thread-title-input');
  let inputForm = $input.parent();
  let $delete = $('#delete-thread');
  let $deleteConfirm = $('#delete-thread-confirm');
  let $save = $('#edit-thread-save');

  let $edit = $('#edit-thread');
  let $cancel = $('#edit-thread-cancel');

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
}

function addPostCallbacks($fullPost) {
  let $post = $fullPost.find('.post:first');
  let threadId = $post.data('threadId');
  let postId = $post.data('postId');

  // region * handle editing
  let $editButton = $post.find('.edit-button');
  $editButton.click(() => {
    let $content = $post.find('.content');
    let $save = $('<button class="btn btn-sm btn-secondary">save</button>');
    let $cancel = $('<button class="btn btn-sm btn-outline-secondary ml-1">cancel</button>');
    let $textarea = $('<textarea class="form-control m-0 p-0 pl-1" style="resize: none; height: 26px"></textarea>');
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

    $editButton.parent().removeClass('show'); // hide the dropdown
    let $postControls = $post.find('.post-controls');
    $postControls.children().addClass('d-none');
    $postControls.append($save);
    $postControls.append($cancel);

    $save.click(() => {
      if (!validate($textarea, doValidate, true)) {
        return;
      }
      let content = $textarea.val().trim();
      fetch('/' + threadId + '/' + postId + '/edit', {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        method: 'POST',
        body: "content=" + $textarea.val().trim()
      })
        .then(r => {
          if (r.ok) {
            return r.text();
          }
          throw new Error('failed to edit')
        })
        .then(text => {
          doValidate.it = false;
          location.reload();
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
      $postControls.children().removeClass('d-none');
    });
  });
  // endregion

  // region * handle replies
  $post.find('.reply-button').click(() => {
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
      $('<div class="collapse" data-post-id="' + postId + '">' + // store the postId here to compare later
        '<div class="pt-2">' +
        '<textarea class="form-control form-control-sm reply-content pr-5"' +
        ' placeholder="Post your reply"' +
        ' name="content"' +
        ' style="resize: none; height: 31px"/>' +
        '<div class="my-2">' +
        '<button class="btn btn-sm btn-secondary post-button">post</button>' +
        '</div>' +
        '</div>' +
        '</div>');
    // endregion

    let $textarea = $reply.find('textarea');
    let $postButton = $reply.find('.post-button');

    // region * auto-post when pressing ctrl+enter
    $textarea.keydown(e => {
      if (e.keyCode === 13 && e.ctrlKey) {
        $postButton.click();
      }
    });
    // endregion

    let doValidate = {it: false};
    $textarea.on('input', () => {
      autoresize($textarea);
      validate($textarea, doValidate);
    });

    $postButton.click(() => {
      if (!validate($textarea, doValidate, true)) {
        return;
      }

      fetch('/' + threadId + '/' + postId, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        method: 'POST',
        body: "content=" + $textarea.val().trim()
      })
        .then(r => {
          if (r.ok) {
            return r.text();
          }
          throw new Error('failed posting reply');
        })
        .then(() => location.reload(), console.error);
    });
    // textarea focus when you press the reply button
    $reply.on('show.bs.collapse', () => setTimeout(() => $textarea.focus(), 0));

    shownReply = $reply;
    $post.next().prepend($reply);
    $reply.collapse('show');
  });
  // endregion

  // region * handle deleting and restoring
  $post.find('[data-post-call]').click(e => {
    fetch(e.target.dataset.postCall, {method: 'POST'})
      .then(r => r.text())
      .then(text => location.reload(), console.error);
  });
  // endregion

  // region * handle references
  const scrollAnimationTime = 400;
  $post.find('[data-post-reference]').click(e => {
    let $reference = $(e.target.dataset.postReference);
    let animated = false;

    let windowHeight = window.innerHeight || document.documentElement.clientHeight;
    let top = $reference.offset().top;
    if (top < $(window).scrollTop()) {
      $('html, body').animate({scrollTop: top - 10}, scrollAnimationTime);
      animated = true;
    } else if (top + $reference.height() > $(window).scrollTop() + windowHeight) {
      $('html, body').animate({scrollTop: top - windowHeight + $reference.height() + 10}, scrollAnimationTime);
      animated = true;
    }

    setTimeout(() => {
      $reference.removeClass('target-fade').addClass('target-fade');
      setTimeout(() => $reference.removeClass('target-fade'), 700);
    }, animated ? scrollAnimationTime : 0);
  });
  // endregion

  if ($fullPost.data('tree-events-handled')) {
    return;
  }
  $fullPost.data('tree-events-handled', true);

  // region * handle collapsing tree
  $fullPost.find('.posts-hidden').click(e => {
    let $msg = $(e.currentTarget);
    let $post = $msg.next();
    let $threadline = $msg.parent().prev();

    $post.collapse('show');
    $msg.collapse('hide');
    $threadline.removeClass('collapsed');
  });
  $fullPost.find('.threadline').click(e => {
    let $threadline = $(e.target);
    let $msg = $threadline.next().children(':first');
    let $post = $msg.next();

    $post.collapse('hide');
    $msg.collapse('show');
    $threadline.addClass('collapsed');
  });
  // endregion
}

function enableStorScrollPosition() {
  $(window).on('beforeunload', () => localStorage.setItem('scroll', $(document).scrollTop()));
  let scroll = localStorage.getItem('scroll');
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }
}

function enableValidation() {
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
}

function enableHotKeySubmit() {
  let $input = $('#thread-title-input');
  let $cancel = $('#edit-thread-cancel');
  let $save = $('#edit-thread-save');
  // 'ctrl+enter' => save the edit and 'escape' => cancel editing
  $input.keydown((e) => {
    if (e.keyCode === 13 && e.ctrlKey) {
      e.preventDefault();
      $save.click();
    } else if (e.keyCode === 27) {
      $cancel.click();
    }
  });

  $('[data-post-button]').keydown(e => {
    if (e.keyCode === 13) {
      if (e.ctrlKey) {
        $(e.target.dataset.postButton).click();
      }
      if (e.target.tagName !== 'TEXTAREA') {
        e.preventDefault();
      }
    }
  });
}

function replaceAppStoreUrl() {
  // region * handle login button
  let $loginButton = $('#login_button');
  $loginButton.attr('href', $loginButton.attr('href') + '?redirectURI=' + encodeURIComponent(location.origin) + '/auth/authorize%3Forigin=' + encodeURIComponent(location.href));
  // endregion
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
  let $stub = $('<div style="height: ' + $textarea.height() + 'px"></div>');
  $textarea.after($stub);
  $textarea.css('height', '1px');
  $textarea.css('height', (2 + $textarea[0].scrollHeight) + 'px');
  $stub.remove();
}

function handleFiles(customFile) {
  customFile.change(function () {
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
}

function enableHandlingRootEdit() {
  $('[data-root-edit]').click((e) => {
    let element = $(e.target.dataset.rootEdit);
    let save = $(`<button class="btn btn-link text-primary";">save</button>`);
    let cancel = $(`<button class="btn btn-link text-secondary mx-1">cancel</button>`);
    let textarea = $(`<textarea maxlength="${e.target.dataset.maxlength ? e.target.dataset.maxlength : 120}" required class="form-control box-shadow" ></textarea>`);
    let attachments = $(`#attachments`);
    let progressBar = $('#progress');

    attachments.show();
    textarea.keydown((e) => {
      if (e.keyCode === 13 && e.ctrlKey) {
        e.preventDefault();
        save.click();
      }
      if (e.keyCode === 27) {
        cancel.click();
      }
    });
    save.click(() => {
      const deleteAttachments = $('[id^="attachment_"]').filter(':checked').map((item, value) => value.dataset.value).get();
      const newAttachments = $('[id$="_attachment"]').get();
      const formData = new FormData();
      window.onbeforeunload = function () {
        return true;
      };
      formData.append("deleteAttachments", deleteAttachments);
      formData.append("content", textarea.val());
      newAttachments.forEach(attachment => {
        if (attachment.files.length) {
          Array.from(attachment.files).forEach(file => {
            formData.append(attachment.name, file)
          })
        }
      });

      progressBar.parent().closest('div').css("display", "");
      textarea.remove();
      cancel.remove();
      save.remove();
      attachments.hide();
      const xhr = new XMLHttpRequest();
      xhr.onload = (exc) => {
        if (xhr.readyState === 4) {
          window.onbeforeunload = null;
          if (xhr.status === 200) {
            location.reload();
          } else {
            console.error(exc);
          }
        }
      };
      xhr.upload.addEventListener('progress', (event) => {
        const progress = Math.round(event.loaded / event.total * 100);
        progressBar.css("width", progress + "%");
        progressBar.html(progress + "%");
      });
      progressBar.show();
      xhr.open("POST", e.target.dataset.editAction, true);
      xhr.send(formData);
    });

    element.after(textarea);
    textarea.focus();
    textarea.val(element[0].dataset.value);
    resizeTextArea(textarea);
    element.addClass('d-none');
    textarea.on('input', () => {
      let lines = textarea.val().split('\n').length;
      textarea.attr('rows', lines);
    });

    let dropdown = $(e.target.parentElement);
    dropdown.removeClass('show');

    let row = dropdown.parent().parent();
    row.children().addClass('d-none');
    row.append(cancel);
    row.append(save);

    cancel.click(() => {
      window.onbeforeunload = null;
      attachments.hide();
      textarea.remove();
      cancel.remove();
      save.remove();
      element.removeClass('d-none');
      row.children().removeClass('d-none');
    });
  });
}

function enableHandlingPagination() {
  function getParams() {
    return location.search
      .slice(1)
      .split('&')
      .filter(value => value)
      .map(p => p.split('='))
      .reduce((obj, [key, value]) => ({...obj, [key]: value}), {});
  }

  const params = getParams();
  const pagination = $("#pagination");
  const currentPage = Number(params.page);
  if (pagination != null && !isNaN(currentPage) && currentPage > 0) {
    const firstPage = $("#firstPage");
    const prevPage = currentPage - 1;
    let maxElements = Number(pagination[0].dataset.maxElements);
    if (maxElements !== 0) {
      const firstPageNumber = prevPage <= 0 ? currentPage : prevPage;
      firstPage[0].href = window.location.pathname + "?page=" + firstPageNumber + "&size=" + params.size;
      firstPage[0].text = firstPageNumber;
      maxElements -= Math.min(maxElements, (prevPage <= 0 ? params.page : (params.page - 1)) * params.size);
      if (prevPage <= 0) {
        firstPage.parent().addClass("active")
      }

      const secondPageImages = Math.min(maxElements, params.size);
      maxElements -= secondPageImages;
      const secondPage = $("#secondPage");
      const secondPageNumber = prevPage <= 0 ? currentPage + 1 : currentPage;
      secondPage[0].href = window.location.pathname + "?page=" + secondPageNumber + "&size=" + params.size;
      secondPage[0].text = secondPageNumber;
      if (prevPage > 0) {
        secondPage.parent().addClass("active");
      } else {
        if (secondPageImages <= 0) {
          secondPage.parent().addClass("disabled");
        }
      }

      const thirdPageImages = Math.min(maxElements, params.size);
      const third = $("#thirdPage");
      const thirdPageNumber = prevPage <= 0 ? currentPage + 2 : currentPage + 1;
      third[0].href = window.location.pathname + "?page=" + thirdPageNumber + "&size=" + params.size;
      third[0].text = thirdPageNumber;
      if (thirdPageImages === 0) {
        third.parent().addClass("disabled");
      }
    } else {
      pagination.css({"display" : "none"});
    }
  } else {
    pagination.css({"display" : "none"});
  }

  const pageSizeSelect = $("#pageSizeSelect");
  let values = $.map(pageSizeSelect.find("option"), function (option) {
    if (Number(pagination[0].dataset.maxElements) < Number($(option).val())) {
      $(option).prop('disabled', true);
    }
    return option.value;
  });
  if (values.includes(params.size)) {
    pageSizeSelect.val(params.size);
  }

  pageSizeSelect.click(() => {
    const size = pageSizeSelect[0].selectedOptions[0].text;
    window.location.href = window.location.pathname + "?page=" + params.page + "&size=" + size
  });
}

function resizeTextArea($element) {
  $element.height($element[0].scrollHeight);
}
