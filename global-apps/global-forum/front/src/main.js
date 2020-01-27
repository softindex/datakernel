import $ from 'jquery';

import 'bootstrap/js/dist/modal';
import 'bootstrap/js/dist/dropdown';
import 'bootstrap/js/dist/popover';

import 'bootstrap/dist/css/bootstrap.min.css';

import 'tempusdominus-bootstrap-4/build/js/tempusdominus-bootstrap-4.min';
import 'tempusdominus-bootstrap-4/build/css/tempusdominus-bootstrap-4.min.css';

import '@fortawesome/fontawesome-free/js/fontawesome';
import '@fortawesome/fontawesome-free/js/solid';

import {setupAttachments} from 'global-comm/src/attachments';
import {setupPost} from 'global-comm/src/posts';

import {
  setupAutocopies,
  setupDatetimePicker,
  setupFormValidation,
  setupLoginButtonUrl,
  setupSubmitKeybind,
  setupThreadTitleControls
} from 'global-comm/src/setups';

setupAttachments($(document));

$('.full-post').each((_, p) => setupPost($(p)));

setupLoginButtonUrl();
setupThreadTitleControls();
setupSubmitKeybind();
setupAutocopies();
setupDatetimePicker();
setupFormValidation();
