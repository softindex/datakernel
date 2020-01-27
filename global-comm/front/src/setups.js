import $ from 'jquery';

import 'bootstrap/js/dist/popover';

import 'tempusdominus-bootstrap-4/build/js/tempusdominus-bootstrap-4.min'
import 'tempusdominus-bootstrap-4/build/css/tempusdominus-bootstrap-4.min.css'

export default function setup() {
  setupLoginButtonUrl();
  setupThreadTitleControls();
  setupSubmitKeybind();
  setupAutocopies();
  setupDatetimePicker();
  setupFormValidation();
}

export function validate($textarea, doValidate, set) {
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

export function autoresize($textarea) {
  const $stub = $(`<div style="height: ${$textarea.height()}px"></div>`);
  $textarea.after($stub);
  $textarea.css('height', '1px');
  void $textarea[0].offsetHeight;
  $textarea.css('height', `${2 + $textarea[0].scrollHeight}px`);
  $stub.remove();
}

export function setupLoginButtonUrl() {
  $('#login_button')
    .attr('href', (_, old) => `${old}?redirectURI=${encodeURIComponent(location.origin)}/authorize%3Forigin=${encodeURIComponent(location.href)}`);
}

export function setupDatetimePicker() {
  $('.date').datetimepicker({
    format: 'HH:mm:ss/DD.MM.YYYY',
    useCurrent: false,
  });
}

export function setupFormValidation() {
  $('.validate').submit(e => {
    const form = e.target;
    if (!form.checkValidity()) {
      e.preventDefault();
      e.stopPropagation();
      form.classList.add('was-validated');
    }
  });
}

export function setupAutocopies() {
  const $autocopied = $('.autocopy');
  $autocopied.popover({
    content: 'Copied to clipboard',
    placement: 'left',
  });
  $autocopied.click(e => {
    const target = $(e.target);
    e.target.select();
    e.target.setSelectionRange(0, 99999); /* For mobile devices */
    document.execCommand('copy');
    setTimeout(() => target.popover('hide'), 1000);
    e.preventDefault();
    return false;
  });
}

export function setupSubmitKeybind() {
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

export function setupThreadTitleControls() {
  const $title = $('#thread-title');
  const $input = $('#thread-title-input');
  const $inputForm = $input.parent();

  const $edit = $('#edit-thread');
  const $cancel = $('#edit-thread-cancel');

  const $delete = $('#delete-thread');
  const $deleteConfirm = $('#delete-thread-confirm');
  const $save = $('#edit-thread-save');

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
    $inputForm.removeClass('was-validated');

    $edit.removeClass('d-none');
    $cancel.addClass('d-none');

    $delete.removeClass('d-none');
    $save.addClass('d-none');
  });

  $save.click(() => {
    const threadId = $save.data('threadId');

    const renamed = $input.val().trim();
    if (renamed.length === 0 || renamed.length > 120) {
      $inputForm.submit();
      return;
    }

    $title.text(renamed);

    const params = new URLSearchParams();
    params.append('title', renamed);
    fetch('/' + threadId + '/rename', {method: 'POST', body: params})
      .then(() => $cancel.click(), console.error);
  });

  $deleteConfirm.click(() => {
    const threadId = $deleteConfirm.data('threadId');

    fetch('/' + threadId, {method: 'DELETE'})
      .then(() => location.href = '/', console.error);
  });
}
