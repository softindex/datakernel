import $ from 'jquery';

import 'bootstrap/js/dist/collapse';

import {addAttachments, createPostAttachmentsPanel} from './attachments';
import {autoresize, validate} from './setups';
import {showImageModal, showVideoModal} from './modals';

import './posts.css';

let $shownReply = null;

export function setupPostEditing($post, threadId, postId, responseHandler) {
  const $editButton = $post.find('.edit-button');

  $editButton.click(() => {
    if ($shownReply) {
      $shownReply.collapse('hide');
    }

    const $content = $post.find('.content');

    const $attachmentsPanel = createPostAttachmentsPanel('mb-1');
    const $attachmentsContainer = $('<div class="row"></div>').append($attachmentsPanel);

    const $save = $('<button class="btn btn-sm btn-secondary">save</button>');
    const $attach = $('<button class="btn btn-sm btn-outline-secondary ml-1">attach</button>')
      .click(() => $attachmentsPanel.collapse('toggle'));
    const $cancel = $('<button class="btn btn-sm btn-outline-secondary ml-1">cancel</button>');

    const editSource = $content.data('edit-src');
    const $editSource = editSource ? $(editSource) : $content;

    const $textarea = $('<textarea class="form-control mb-2 p-0 pl-1" style="resize: none; height: 26px"></textarea>')
      .focus()
      .val($editSource.text());

    const doValidate = {it: false};
    $content.after($textarea).addClass('d-none');
    autoresize($textarea);

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

    const $postControls = $post.find('.post-controls');

    $postControls.children().addClass('d-none');

    $postControls
      .before($attachmentsContainer)
      .append($save)
      .append($attach)
      .append($cancel);

    const $shownAttachments = $post.find('.attachments');

    const wasHidden = $shownAttachments.hasClass('d-none');
    if (wasHidden) {
      $shownAttachments.removeClass('d-none');
    }

    const $downloadButtons = $shownAttachments.find('.attachment-action.download');
    const $deleteButtons = $shownAttachments.find('.attachment-action.trash');

    $downloadButtons.addClass('d-none');
    $deleteButtons.removeClass('d-none');

    $deleteButtons.click(e => $(e.currentTarget).toggleClass('active'));

    $save.click(() => {
      if (!validate($textarea, doValidate, true)) {
        return;
      }
      const formData = new FormData();
      const content = $textarea.val().trim();
      formData.append('content', content);

      addAttachments(formData, $attachmentsContainer);

      const removedAttachments = [];
      $post.find('.attachment-action.trash')
        .each((_, e) => {
          const $e = $(e);
          if ($e.hasClass('active')) {
            removedAttachments.push($e.parent().children('.attachment-title').text());
          }
        });
      formData.append('removeAttachments', removedAttachments.join(','));

      fetch(`/${threadId}/${postId}/edit`, {method: 'POST', body: formData})
        .then(r => {
          if (r.ok) {
            return r.text();
          }
          throw new Error('failed to edit')
        })
        .then(text => {
          doValidate.it = false;
          $downloadButtons.removeClass('d-none');
          $deleteButtons.addClass('d-none');
          $attachmentsContainer.remove();
          if (wasHidden) {
            $shownAttachments.addClass('d-none');
          }

          responseHandler(text);
        }, e => {
          console.error(e);
          $cancel.click();
        });
    });

    $cancel.click(() => {
      $(document).focus();
      $attachmentsContainer.remove();
      if (wasHidden) {
        $shownAttachments.addClass('d-none');
      }
      $textarea.remove();
      $attach.remove();
      $cancel.remove();
      $save.remove();
      $content.removeClass('d-none');
      $postControls.children().removeClass('d-none');
      $downloadButtons.removeClass('d-none');
      $deleteButtons.addClass('d-none');
    });
  });
}

export function setupReplies($post, threadId, postId, responseHandler) {
  $post.find('.reply-button').click(() => {

    // closing the opened reply
    if ($shownReply !== null) {
      $shownReply.collapse('hide');

      // if it's the same reply then we do nothing more
      if ($shownReply.data('post-id') === postId) {
        return;
      }
    }

    const doValidate = {it: false};

    const $attachmentPanel = createPostAttachmentsPanel('mt-2');

    const $postButton = $(`<button class="btn btn-sm btn-secondary">post</button>`)
      .click(() => {
        if (!validate($textarea, doValidate, true)) {
          return;
        }
        const formData = new FormData();
        formData.append('content', $textarea.val().trim());
        addAttachments(formData, $attachmentPanel);
        fetch(`/${threadId}/${postId}`, {method: 'POST', body: formData})
          .then(r => {
            if (r.ok) {
              return r.text();
            }
            throw new Error('failed posting reply');
          })
          .then(text => {
            $reply.remove();
            $shownReply = null;
            responseHandler(text);
          }, console.error);
      });

    const $attachButton = $(`<button class="ml-1 btn btn-sm btn-outline-secondary">attach</button>`)
      .click(() => $attachmentPanel.collapse('toggle'));

    const $textarea = $(`<textarea class="form-control form-control-sm reply-content pr-5" placeholder="Post your reply" name="content" style="resize: none; height: 31px"></textarea>`)
      .keydown(e => {
        if (e.keyCode === 13 && e.ctrlKey) {
          $postButton.click();
        }
      })
      .on('input', () => {
        autoresize($textarea);
        validate($textarea, doValidate);
      });

    const $reply = $(`<div class="collapse"></div>`)
      .append($(`<div class="pt-2"></div>`)
        .append($textarea)
        .append($attachmentPanel)
        .append($(`<div class="my-2"></div>`)
          .append($postButton)
          .append($attachButton)))
      .data('post-id', postId)
      .on('show.bs.collapse', e => {
        // ugh
        if (e.target !== e.currentTarget) return;
        setTimeout(() => $textarea.focus(), 0);
      })
      .on('hidden.bs.collapse', e => {
        if (e.target !== e.currentTarget) return;
        $reply.remove();
        if ($shownReply === $reply) {
          $shownReply = null;
        }
      });

    $shownReply = $reply;
    $post.next().prepend($reply);
    $reply.collapse('show');
  });
}

function setupPostCalls($post) {
  $post.find('[data-post-call]').click(e => {
    fetch(e.currentTarget.dataset.postCall, {method: 'POST'})
      .then(r => r.text())
      .then(text => {
        const $rerendered = $(text);
        $post.replaceWith($rerendered);
        setupPost($rerendered.parents('.full-post:first'));
      }, console.error);
  });
}

function setupPostRatings($post) {
  $post.find('.like.active').click(e => {
    const $parent = $(e.currentTarget).parent();
    const prefix = $parent.data('prefix');
    if (!prefix) {
      debugger;
    }
    const $rating = $parent.find('.rating');
    if ($parent.hasClass('liked')) {
      fetch(`${prefix}/rate/null`, {method: 'POST'})
        .then(() => {
          $parent.removeClass('liked');
          $rating.text((_, text) => parseInt(text) - 1);
        }, console.error)
    } else {
      fetch(`${prefix}/rate/like`, {method: 'POST'})
        .then(() => {
          $parent.addClass('liked');
          if ($parent.hasClass('disliked')) {
            $parent.removeClass('disliked');
            $rating.text((_, text) => parseInt(text) + 2);
          } else {
            $rating.text((_, text) => parseInt(text) + 1);
          }
        }, console.error)
    }
  });
  $post.find('.dislike.active').click(e => {
    const parent = $(e.currentTarget).parent();
    const prefix = parent.data('prefix');
    const child = parent.find('.rating');
    if (parent.hasClass('disliked')) {
      fetch(`${prefix}/rate/null`, {method: 'POST'})
        .then(() => {
          parent.removeClass('disliked');
          child.text((_, text) => parseInt(text) + 1);
        }, console.error)
    } else {
      fetch(`${prefix}/rate/dislike`, {method: 'POST'})
        .then(() => {
          parent.addClass('disliked');
          if (parent.hasClass('liked')) {
            parent.removeClass('liked');
            child.text((_, text) => parseInt(text) - 2);
          } else {
            child.text((_, text) => parseInt(text) - 1);
          }
        }, console.error)
    }
  });
}

function setupReferences($post, scrollAnimationTime) {
  $post.find('[data-post-reference]').click(e => {
    const $reference = $(e.currentTarget.dataset.postReference);

    let animated = false;

    const windowHeight = window.innerHeight || document.documentElement.clientHeight;
    const top = $reference.offset().top;
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
}

function setupPostTreeCollapsing($fullPost) {
  if ($fullPost.data('tree-events-handled')) {
    return;
  }
  $fullPost.data('tree-events-handled', true);
  $fullPost.find('.posts-hidden').click(e => {
    const $msg = $(e.currentTarget);
    const $post = $msg.next();
    const $threadline = $msg.parent().prev();

    $post.collapse('show');
    $msg.collapse('hide');
    $threadline.removeClass('collapsed');
  });
  $fullPost.find('.threadline').click(e => {
    const $threadline = $(e.currentTarget);
    const $msg = $threadline.next().children(':first');
    const $post = $msg.next();

    $msg.children(':first').text(`(${$post.find('.post').length} hidden)`);

    $post.collapse('hide');
    $msg.collapse('show');

    $threadline.addClass('collapsed');
  })
}

export function setupPost($fullPost) {
  if ($fullPost.length === 0) {
    return;
  }
  const $post = $fullPost.find('.post:first');
  const threadId = $post.data('threadId');
  const postId = $post.data('postId');

  // attachment title hack
  $post.find('.attachment-title').text((_, text) => text.length > 64 ? `${text.substring(0, 61)}...` : text);

  // modals opening
  $post.find('[data-image-modal]').click(async e => e.currentTarget.tagName !== 'A' && await showImageModal(e.currentTarget.dataset.imageModal));
  $post.find('[data-video-modal]').click(e => e.currentTarget.tagName !== 'A' && showVideoModal(e.currentTarget.dataset.videoModal));

  setupPostEditing($post, threadId, postId, text => {
    const $rerendered = $(text);
    $post.replaceWith($rerendered);
    setupPost($rerendered.parents('.full-post:first'));
  });

  setupReplies($post, threadId, postId, response => {
    const $rerendered = $(response);
    setupPost($rerendered); // this one is a full post already
    if ($rerendered.find('.post').data('inlineParent')) {
      $fullPost.parent().append($rerendered);
    } else {
      $fullPost.find('.post-children:first').append($rerendered);
    }
  });

  setupPostCalls($post); // deleting and restoring
  setupPostRatings($post); // likes and dislikes
  setupReferences($post, 400);
  setupPostTreeCollapsing($fullPost);
}

export default function setupAllPosts() {
  $('.full-post').each((_, p) => setupPost($(p)));
}
