import $ from 'jquery';
import {setupPost, setupPostEditing, setupReplies} from 'global-comm/src/posts';


export function setupBlogPost($rootPost) {
  const threadId = $rootPost.data('thread-id');

  setupReplies($rootPost, threadId, 'root', response => {
    const $rerendered = $(response);
    setupPost($rerendered);
    $rootPost.next().append($rerendered);
  });

  setupPostEditing($rootPost, threadId, 'root', response => {
    const $rerendered = $(response);
    setupBlogPost($rerendered);
    $rootPost.replaceWith($rerendered);
  });
}
