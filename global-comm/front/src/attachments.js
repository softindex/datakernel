import $ from 'jquery';

export function addAttachments(formData, $addAttachments) {
  function addAll(formData, fieldName, $input) {
    let files = $input[0].files;
    for (let i = 0; i < files.length; i++) {
      let f = files[i];
      formData.append(fieldName, f, f.name);
    }
    $input.val('');
    $input.trigger('change');
  }

  let $imageAttachment = $addAttachments.find('.image-attachment');
  let $videoAttachment = $addAttachments.find('.video-attachment');
  let $documentAttachment = $addAttachments.find('.document-attachment');

  addAll(formData, 'image_attachment', $imageAttachment);
  addAll(formData, 'video_attachment', $videoAttachment);
  addAll(formData, 'document_attachment', $documentAttachment);
}

export function setupAttachments($element) {
  $element.find('input[type=file]')
    .change(e => {
      const input = e.target;
      const files = input.files;
      const type = input.name.split('_')[0];
      const numberOfFiles = files.length;
      const label = $(input).next('label');

      if (numberOfFiles > 1) {
        label.html(`[${numberOfFiles} ${type}s]`)
      } else if (numberOfFiles === 1) {
        label.html(files[0].name.split('\\').pop());
      } else if (numberOfFiles === 0) {
        label.html(`Attach ${type}s`);
      }
    });
}

export function createPostAttachmentsPanel(containerClasses) {
  const $panel = $(
    `<div class="collapse">` +
    `<div class="${containerClasses}">` +
    `<div class="custom-file col-3 mr-1">` +
    `<input type="file" accept="image/*" class="custom-file-input image-attachment" multiple` +
    ` name="image_attachment">` +
    `<label class="custom-file-label text-truncate">Attach images</label>` +
    `</div>` +
    `<div class="custom-file col-3 mx-1">` +
    `<input type="file" accept="video/mp4" class="custom-file-input video-attachment" multiple` +
    ` name="video_attachment">` +
    `<label class="custom-file-label text-truncate">Attach videos</label>` +
    `</div>` +
    `<div class="custom-file col-3 ml-1">` +
    `<input type="file" class="custom-file-input document-attachment"  multiple` +
    ` name="document_attachment">` +
    `<label class="custom-file-label text-truncate">Attach documents</label>` +
    `</div>` +
    `</div>` +
    `</div>`
  );
  setupAttachments($panel);
  return $panel;
}
