let shownReply = null;

window.onload = () => {
  $('.full-post').each((_, p) => addPostCallbacks($(p)));

  enableHandlingAttachments();
  enableHandlingEdit();
  enableHandlingPagination();
  enableSelectingComments();
  enableAutoresizeTextarea();
  enableResolutionSelector();
  enableHandlingPoster();
  enableProgressBar();
  enableHandlingLoginButton();
  enableValidation();
  enableStoreScrollPosition();
  enableUploadNewView();
  handleAttachments($(document));
};

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

function prevPage() {
  const getParams = location.search
    .slice(1)
    .split('&')
    .filter(value => value)
    .map(p => p.split('='))
    .reduce((obj, [key, value]) => ({...obj, [key]: value}), {});
  window.location.href = window.location.origin + window.location.pathname + "?page=" + (getParams.page > 1 ? getParams.page - 1 : 1) + "&size=" + getParams.size;
}

function nextPage(maxElements) {
  const getParams = location.search
    .slice(1)
    .split('&')
    .filter(value => value)
    .map(p => p.split('='))
    .reduce((obj, [key, value]) => ({...obj, [key]: value}), {});
  const nextPage = Number(getParams.page) + 1;
  const currentSize = getParams.page * getParams.size;
  window.location.href = window.location.origin + window.location.pathname + "?page=" + (currentSize >= maxElements ? getParams.page : nextPage) + "&size=" + getParams.size;
}

function enableResolutionSelector() {
  const modalVideoTag = $("#modalVideoTag");
  $("#resolution_selector").change(function () {
    $("#resolution_selector option:selected").each(function () {
      modalVideoTag.attr("src", $(this).val());
    });
  });
}

function enableHandlingAttachments() {
  $('#video_attachment').on('change',
    function () {
      const videoPreview = $('#videoPreview');
      videoPreview.attr('src', URL.createObjectURL(this.files[0]));
      videoPreview.parent().show();
    });
}

function resizeTextArea($element) {
  $element.height($element[0].scrollHeight);
}

function enableHandlingEdit() {
  $('[data-edit]').click((e) => {
    let element = $(e.target.dataset.edit);
    let save = $(`<button class="btn btn-sm btn-primary">save</button>`);
    let cancel = $(`<button class="btn btn-sm btn-outline-primary mx-1">cancel</button>`);
    let textarea = $(`<textarea maxlength="${e.target.dataset.maxlength ? e.target.dataset.maxlength : 120}" required class="form-control" ></textarea>`);

    save.click(() => {
      fetch(e.target.dataset.editAction, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        method: 'POST',
        body: "content=" + textarea.val()
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
      pagination.toggle("hide");
    }
  } else {
    pagination.toggle("hide");
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

function enableSelectingComments() {
  const posts = $("span[id^='comment_']");
  posts.each((index, elem) => {
    elem.onclick = function (e) {
      const divPosts = $("div[id='" + elem.id + "']");
      divPosts.addClass("target-fade");
      setTimeout(() => divPosts.removeClass('target-fade'), 1500);
    }
  });
}

function enableHandlingPoster() {
  $("#poster_attachment").change(function () {
    if (this.files) {
      Array.from(this.files).forEach(file => {
        const reader = new FileReader();
        reader.onload = function (e) {
          const $newPoster = $("#new_poster");
          $newPoster.attr("src", e.target.result);
          $newPoster.css("background", "whitesmoke");
        };
        reader.readAsDataURL(file);
      });
    }
  });
}

function enableProgressBar() {
  const pending_task = $("div[id^=pending_task_]");
  if (pending_task.length !== 0) {
    let round2dec = num => Math.round(num * 100);
    const source = new EventSource("/progress");
    source.onmessage = function (event) {
      pending_task.each((index, task) => {
        const jsonData = JSON.parse(event.data);
        const status = $("#status_task_" + task.dataset.id);
        const progress = jsonData[task.dataset.id];
        // noinspection EqualityComparisonWithCoercionJS
        if (progress == 0.0 || progress == null)   {
          $(status[0]).text("");
        } else {
          // noinspection EqualityComparisonWithCoercionJS
          if (progress == 1) {
            setTimeout(function () {
              location.reload();
            }, 5000);
            $(status[0]).text("wait...");
            source.close();
          } else {
            $(status[0]).text(round2dec(progress) + "%");
          }
        }
      });
    };
    source.onerror = function () {
      setTimeout(function () {
        location.reload();
      }, 10000);
    }
  }
}


function enableStoreScrollPosition() {
  $(window).on('beforeunload', () => localStorage.setItem('scroll', $(document).scrollTop()));
  let scroll = localStorage.getItem('scroll');
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }
}

function enableHandlingLoginButton() {
  let $loginButton = $('#login_button');
  $loginButton.attr('href', $loginButton.attr('href') + '?redirectURI=' + encodeURIComponent(location.origin) + '/auth/authorize%3Forigin=' + encodeURIComponent(location.href));
}

function enableAutoresizeTextarea() {
  const description = $("#description-field");
  if (description[0] !== undefined) {
    resizeTextArea(description);
  }
}

function autoresize($textarea) {
  let $stub = $('<div style="height: ' + $textarea.height() + 'px"></div>');
  $textarea.after($stub);
  $textarea.css('height', '1px');
  $textarea.css('height', (2 + $textarea[0].scrollHeight) + 'px');
  $stub.remove();
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
      fetch('/private/thread/' + threadId + '/' + postId + '/edit', {
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

    let doValidate = {it: false};
    $textarea.on('input', () => {
      autoresize($textarea);
      validate($textarea, doValidate);
    });

    $postButton.click(() => {
      if (!validate($textarea, doValidate, true)) {
        return;
      }

      fetch('/private/thread/' + threadId + '/' + postId, {
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

function enableUploadNewView() {
  const uploadImages = $("#upload_video");
  const forms = document.getElementsByClassName('needs-validation');
  const validation = Array.prototype.filter.call(forms, function(form) {
    form.addEventListener('submit', function(event) {
      event.preventDefault();
      event.stopPropagation();
      if (form.checkValidity() === false) {
        form.classList.add('was-validated');
      } else {
        const forms = $(".needs-validation");

        let progressBar = $('#progress');
        const videoAttachment = $('[id$="video_attachment"]').get();
        const formData = new FormData();
        uploadImages.hide();
        const $uploadTitle = $("#upload_title");
        const $uploadContent = $("#upload_content");
        $uploadTitle.attr("disabled", true);
        $uploadContent.attr("disabled", true);
        $(".custom-file").hide();

        formData.append("title", $uploadTitle[0].value);
        formData.append("content", $uploadContent[0].value);
        videoAttachment.forEach(attachment => {
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
        xhr.open("POST", $(uploadImages)[0].dataset.url, true);
        xhr.send(formData);
      }
    }, false);
  });
}
