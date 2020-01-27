import $ from 'jquery';

import './modals.css';

const $window = $(window);
const $body = $(document.body);

const $imageModal = $('#image-modal')
  .click(() => {
    $imageModal.addClass('d-none');
    $body.removeClass('no-overflow');
  });

const $imageModalImg = $imageModal.find('img');

const checkOverflow = () => {
  if ($imageModalImg.height() > $window.height()) {
    $imageModal.addClass('overflow');
  } else {
    $imageModal.removeClass('overflow');
  }
};

$window.resize(checkOverflow);

export async function showImageModal(imageUrl) {
  $imageModalImg.attr('src', imageUrl);

  // wait for the dom change to apply
  await new Promise(resolve => setTimeout(resolve, 0));

  const openModal = () => {
    $imageModal.removeClass('d-none');
    checkOverflow();
    $body.addClass('no-overflow');
  };

  if ($imageModalImg.attr('loaded')) {
    openModal();
    return;
  }
  return new Promise(resolve => {
    $imageModalImg.one('load', () => {
      openModal();
      resolve();
    });
  });
}

const $videoModal = $('#video-modal')
  .click(() => {
    $videoModal.addClass('d-none');
    $body.removeClass('no-overflow');
  });

const $videoModalVid = $videoModal.find('video');

export function showVideoModal(videoUrl) {
  $videoModalVid.attr('src', videoUrl).removeClass('d-none');
  $body.addClass('no-overflow');
}
