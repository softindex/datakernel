const allImages = $("img[id]");

$(document).ready(() => {
  preloadImage(allImages);
});

window.onload = () => {
  lazyLoad();
  window.addEventListener("scroll", lazyLoad);
  window.addEventListener("resize", lazyLoad);

  enableZoom();
  enableOnImageTryUploadListener($("#gallery"));

  // ======== creation album handling ======
  const cancelCreationAlbum = $("#cancel_creation_album");
  const createAlbum = $("#create_album");
  const albumForm = $("#new_album_form");
  const titleAlbum = $("#title_album");
  const descriptionAlbum = $("#description_album");
  enableAlbumCreationListener(createAlbum, cancelCreationAlbum, albumForm[0], titleAlbum[0], descriptionAlbum[0]);

  // ======== deletion album handling ======
  const cancelDeletionPhotos = $("#cancelDeletionPhotos");
  const deletePhotosForm = $("#delete_photos_form");
  const deletePhotos = $("#deletePhotos");
  const deletePhotosTool = $("#delete_photos_tool");
  const deleteAlbumButton = $("#delete_album_tool");
  enableAlbumDeletionListener(deletePhotos, deletePhotosForm, cancelDeletionPhotos, deletePhotosTool, deleteAlbumButton);

  // ======== edit photo description handling ======
  const savePhotoDescription = $("#savePhotoDescription");
  const editPhotoDescription = $("#editPhotoDescription");
  const editDescriptionButtonGroup = $("#editDescriptionButtonGroup");
  const photoDescription = $("#photo_description");
  const photoDescriptionStatic = $("#photo_description_static");
  enableEditingDescriptionPhoto(savePhotoDescription, editDescriptionButtonGroup, editPhotoDescription, photoDescription, photoDescriptionStatic);

  // ======== move album handling ======
  const movePhotosForm = $("#move_photos_form");
  const movePhotosTool = $("#move_photos_tool");
  const movePhotos = $("#movePhotos");
  const cancelMovingPhotos = $("#cancelMovingPhotos");
  const albumSelect = $("#albumSelect");
  enableMovingPhotosListener(movePhotos, movePhotosForm[0], cancelMovingPhotos, movePhotosTool, albumSelect);

  // ======== update album handling ======
  const updateAlbumForm = $("#update_album_form");
  const updateAlbumTool = $("#update_album_tool");
  const updateAlbum = $("#update_album");
  const cancelUpdatingAlbum = $("#cancel_updating_album");
  const updateDescriptionAlbum = $("#update_description_album");
  const updateTitleAlbum = $("#update_title_album");
  const currentDescriptionAlbum = $("#current_description_album");
  enableUpdatingAlbum(updateAlbumTool, updateAlbumForm, currentDescriptionAlbum, cancelUpdatingAlbum, updateAlbum, updateTitleAlbum, updateDescriptionAlbum);

  // ======== pagination handling ======
  const pagination = $("#pagination");
  const currentPage = Number(getParams().page);
  const firstPage = $("#firstPage");
  const secondPage = $("#secondPage");
  const thirdPage = $("#thirdPage");
  const pageSizeSelect = $("#pageSizeSelect");
  if (pageSizeSelect[0] !== undefined) {
    enablePaginationHandling(pagination[0], currentPage, firstPage[0], secondPage[0], thirdPage[0], pageSizeSelect);
  }


  // Upload images
  enableUploadImages($("#upload_images"), $("#attachments"));

  // region * handle login button
  let $loginButton = $('#login_button');
  $loginButton.attr('href', $loginButton.attr('href') + '?redirectURI=' + encodeURIComponent(location.origin) + '/auth/authorize%3Forigin=' + encodeURIComponent(location.href));
};

function autoresize($textarea) {
  let $stub = $('<div style="height: ' + $textarea.height() + 'px"></div>');
  $textarea.after($stub);
  $textarea.css('height', '1px');
  $textarea.css('height', (10 + $textarea[0].scrollHeight) + 'px');
  $stub.remove();
}

function preloadImage(allImages) {
  // === Preload image ====
  allImages.each((_, img) => {
    const $img = $(img);

    $img.one("load", () => {
      $img.show();
      $("#svg_" + img.id).remove();
    });
  });
}

function enableOnImageTryUploadListener(gallery) {
  $("#image_attachment").change(function () {
    if (this.files) {
      Array.from(this.files).forEach(file => {
        const reader = new FileReader();
        reader.onload = function (e) {
          gallery.append($(
            "<div class=\"mb-3 pics animation all 2\">" +
            "<img class=\"img-fluid\" src=\"" + e.target.result + "\">" +
            "</div>"))
        };
        reader.readAsDataURL(file);
      });
    }
  });
}

function getParams() {
  return location.search
    .slice(1)
    .split('&')
    .filter(value => value)
    .map(p => p.split('='))
    .reduce((obj, [key, value]) => ({...obj, [key]: value}), {});
}

function enableUploadImages(uploadImages, attachments) {
  uploadImages.click(() => {
    let progressBar = $('#progress');
    const newAttachments = $('[id$="_attachment"]').get();
    const formData = new FormData();
    attachments.hide();
    newAttachments.forEach(attachment => {
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
  });
}

function enablePaginationHandling(pagination, currentPage, firstPage, secondPage, thirdPage, pageSizeSelect) {
  const params = getParams();
  if (pagination != null && !isNaN(currentPage) && currentPage > 0) {
    const prevPage = currentPage - 1;
    let maxElements = Number(pagination.dataset.maxElements);
    if (maxElements === 0) {
      $(pagination).toggle("hide");
      return;
    }

    const firstPageNumber = prevPage <= 0 ? currentPage : prevPage;
    firstPage.href = window.location.pathname + "?page=" + firstPageNumber + "&size=" + params.size;
    firstPage.text = firstPageNumber;
    maxElements -= Math.min(maxElements, (prevPage <= 0 ? params.page : (params.page - 1)) * params.size);
    if (prevPage <= 0) {
      $(firstPage).parent().addClass("active");
    }

    const secondPageImages = Math.min(maxElements, params.size);
    maxElements -= secondPageImages;
    const secondPageNumber = prevPage <= 0 ? currentPage + 1 : currentPage;
    secondPage.href = window.location.pathname + "?page=" + secondPageNumber + "&size=" + params.size;
    secondPage.text = secondPageNumber;
    if (prevPage > 0) {
      $(secondPage).parent().addClass("active");
    } else {
      if (secondPageImages <= 0) {
        $(secondPage).parent().addClass("disabled");
      }
    }

    const thirdPageImages = Math.min(maxElements, params.size);
    const thirdPageNumber = prevPage <= 0 ? currentPage + 2 : currentPage + 1;
    thirdPage.href = window.location.pathname + "?page=" + thirdPageNumber + "&size=" + params.size;
    thirdPage.text = thirdPageNumber;
    if (thirdPageImages === 0) {
      $(thirdPage).parent().addClass("disabled");
    }
  } else {
    $(pagination).toggle("hide");
  }

  let values = $.map(pageSizeSelect.find("option"), function (option) {
    if (Number(pagination.dataset.maxElements) < Number($(option).val())) {
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

function enableMovingPhotosListener(movePhotos, movePhotosForm, cancelMovingPhotos, movePhotosTool, albumSelect) {
  const uploadImageButton = $("#uploadImageButton");
  const createAlbumButton = $("#create-album");
  const toolsButton = $("#tools_button");
  const tools = $("#tools");
  const imageCheckboxs = $("[id^=image_checkbox_]");
  movePhotos.click(() => {
    const selectedImages = imageCheckboxs.filter('.image-checkbox-checked').map((index, value) => value.dataset.id).get();
    fetch(movePhotosForm.dataset.url, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      },
      body: JSON.stringify([albumSelect.val(), selectedImages]),
      method: 'POST',
    }).then(() => location.reload(), console.error);
  });

  cancelMovingPhotos.click(() => {
    $(movePhotosForm).toggle();
    uploadImageButton.removeClass('disabled');
    createAlbumButton.removeClass('disabled');
    toolsButton.attr("disabled", false);
    enableZoom();
    disableSelection();
  });

  movePhotosTool.click(() => {
    $(movePhotosForm).toggle();
    tools.collapse("hide");
    toolsButton.attr("disabled", true);
    uploadImageButton.addClass('disabled');
    createAlbumButton.addClass('disabled');
    disableZoom();
    enableSelection();
  });
}


function enableEditingDescriptionPhoto(savePhotoDescription, editDescriptionButtonGroup, editPhotoDescription, photoDescription, photoDescriptionStatic) {
  const modal = $("#modal");
  const close = $("#close");
  const albumBar = $("#album_bar");
  const modalImg = $("#modalImg");
  const downloadButton = $("#downloadButton");
  savePhotoDescription.click(() => {
    fetch(modalImg[0].dataset.url, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      },
      body: JSON.stringify([photoDescription.val()]),
      method: 'POST',
    }).then(() => location.reload(), console.error);
  });

  const cancelEditingPhotoDescription = $("#cancelEditingPhotoDescription");
  cancelEditingPhotoDescription.click(() => {
    downloadButton.show();
    if (photoDescriptionStatic.text()) {
      photoDescriptionStatic.show();
    }
    photoDescription.hide();
    editDescriptionButtonGroup.css({display: "none"});
    editPhotoDescription.css({display: ""});
    photoDescription.val(modalImg[0].dataset.description);
    photoDescription.prop('readonly', true);
  });

  editPhotoDescription.click(() => {
    downloadButton.hide();
    photoDescriptionStatic.hide();
    photoDescription.show();
    autoresize(photoDescription);
    editDescriptionButtonGroup.css({display: ""});
    editPhotoDescription.css({display: "none"});
    photoDescription.prop('readonly', false).focus();
  });

  function doClose() {
    photoDescriptionStatic.val("");
    photoDescription.val("");
    editDescriptionButtonGroup.css({display: "none"});
    editPhotoDescription.css({display: ""});
    photoDescription.css({display: "none"});
    albumBar.css({display: ""});
    modal.css({display: "none"});
  }

  modal.click(e => {
    const target = $(e.target);
    if (target.is("div")) {
      doClose();
    }
  });

  close.click(() => {
    doClose();
  });
}


function enableAlbumDeletionListener(deletePhotos, deletePhotosForm, cancelDeletionPhotos, deletePhotosTool, deleteAlbumButton) {
  const uploadImageButton = $("#uploadImageButton");
  const createAlbumButton = $("#create-album");
  const imageCheckboxs = $("[id^=image_checkbox_]");
  const albumBar = $("#album_bar");
  const toolsButton = $("#tools_button");
  const tools = $("#tools");
  deletePhotos.click(() => {
    const selectedImages = imageCheckboxs.filter('.image-checkbox-checked').map((index, value) => value.dataset.id).get();
    if (selectedImages.length === 0) {
      $("#deleteWarningToast").toast('show');
      return;
    }
    fetch(deletePhotosForm[0].dataset.url, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      },
      method: 'POST',
      body: JSON.stringify(selectedImages)
    }).then((response) => {
      if (response.redirected) {
        window.location.href = response.url;
      } else {
        location.reload()
      }
    }, console.error);
  });

  cancelDeletionPhotos.click(() => {
    albumBar.css({display: ""});
    toolsButton.attr("disabled", false);
    createAlbumButton.removeClass("disabled");
    uploadImageButton.removeClass("disabled");
    disableSelection();
    $(deletePhotosForm).toggle();
    enableZoom();
  });

  deletePhotosTool.click(() => {
    albumBar.css({display: "none"});
    createAlbumButton.addClass("disabled");
    uploadImageButton.addClass("disabled");
    tools.collapse("hide");
    toolsButton.attr("disabled", true);
    $(deletePhotosForm).toggle();
    disableZoom();
    enableSelection();
  });

  deleteAlbumButton.click((e) => {
    if (confirm("Are you sure you want to delete this?")) {
      fetch(deleteAlbumButton[0].dataset.url, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        method: 'POST',
      }).then((response) => {
        if (response.redirected) {
          window.location.href = response.url;
        } else {
          location.reload()
        }
      }, console.error);
    } else {
      e.preventDefault();
    }
  });
}

function enableUpdatingAlbum(updateAlbumTool, updateAlbumForm, currentDescriptionAlbum, cancelUpdatingAlbum, updateAlbum, updateTitleAlbum, updateDesciptionAlbum) {
  const uploadImageButton = $("#uploadImageButton");
  const createAlbumButton = $("#create-album");
  const toolsButton = $("#tools_button");
  const tools = $("#tools");
  updateAlbumTool.click(() => {
    updateAlbumForm.toggle();
    tools.collapse("hide");
    toolsButton.attr("disabled", true);
    uploadImageButton.addClass('disabled');
    createAlbumButton.addClass('disabled');
    disableZoom();
    currentDescriptionAlbum.toggle();
  });

  cancelUpdatingAlbum.click(() => {
    updateAlbumForm.toggle();
    uploadImageButton.removeClass('disabled');
    createAlbumButton.removeClass('disabled');
    toolsButton.attr("disabled", false);
    enableZoom();
    currentDescriptionAlbum.toggle();
  });

  Array.prototype.filter.call($(".needs-validation-update"), function (form) {
    form.addEventListener('submit', function (event) {
      event.preventDefault();
      event.stopPropagation();
      if (form.checkValidity() === true) {
        fetch(updateAlbumForm[0].dataset.url, {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          },
          method: 'POST',
          body: JSON.stringify([
            updateTitleAlbum[0].value,
            updateDesciptionAlbum[0].value,
          ])
        }).then((response) => {
          if (response.redirected) {
            window.location.href = response.url;
          } else {
            location.reload()
          }
        }, console.error);
      }
      form.classList.add('was-validated');
    }, false);
  });
}

function enableAlbumCreationListener(createAlbum, cancelCreationAlbum, albumForm, titleAlbum, descriptionAlbum) {
  const uploadImageButton = $("#uploadImageButton");
  const createAlbumButton = $("#create-album");
  const albumBar = $("#album_bar");

  const tools = $("#tools");
  const imageCheckboxs = $("[id^=image_checkbox_]");

  Array.prototype.filter.call($(".needs-validation"), function (form) {
    form.addEventListener('submit', function (event) {
      event.preventDefault();
      event.stopPropagation();
      const selectedImages = imageCheckboxs.filter('.image-checkbox-checked').map((index, value) => value.dataset.id).get();
      if (form.checkValidity() === true && selectedImages.length !== 0) {
        fetch(albumForm.dataset.url, {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          },
          method: 'POST',
          body: JSON.stringify([
            titleAlbum.value,
            descriptionAlbum.value,
            selectedImages
          ])
        })
          .then((response) => {
            if (response.redirected) {
              window.location.href = response.url;
            } else {
              location.reload()
            }
          }, console.error);
      } else {
        const warningToast = $('#warningToast');
        warningToast.toast('show');
      }
      form.classList.add('was-validated');
    }, false);
  });

  cancelCreationAlbum.click(() => {
    $(albumForm).toggle();
    uploadImageButton.removeClass('text-secondary');
    createAlbumButton.removeClass('text-secondary');
    disableSelection();
    enableZoom();
  });

  createAlbumButton.click(() => {
    disableZoom();
    tools.addClass('text-secondary');
    uploadImageButton.addClass('text-secondary');
    createAlbumButton.addClass('text-secondary');
    albumBar.css({display: "none"});
    $(albumForm).toggle();
    enableSelection();
  });
}

function enableSelection() {
  const imageCheckboxs = $("[id^=image_checkbox_]");
  imageCheckboxs.on("click", function (e) {
    $(this).toggleClass('image-checkbox-checked');
    const $checkbox = $(this).find('input[type="checkbox"]');
    $checkbox.prop("checked", !$checkbox.prop("checked"));
    e.preventDefault();
  });
}

function disableSelection() {
  const imageCheckboxs = $("[id^=image_checkbox_]");
  imageCheckboxs.off();
  imageCheckboxs.each(function () {
    $(this).removeClass('image-checkbox-checked');
  });
}

function enableZoom() {
  const modalImg = $("#modalImg");
  const downloadButton = $("#downloadButton");
  const photoDescription = $("#photo_description");
  const photoDescriptionStatic = $("#photo_description_static");
  const allImages = $("img[id]");
  const modal = $("#modal");
  allImages.each((index, img) => {
    img.onclick = () => {
      const $img = $(img);
      downloadButton.attr("href", $img[0].dataset.fullImg);
      modal.css({display: "block"});
      modalImg.attr("src", $img[0].dataset.thumbnailImg);
      modalImg.attr("data-description", $img[0].dataset.description);
      modalImg.attr("data-url", $img[0].dataset.url);
      photoDescriptionStatic.text(modalImg[0].dataset.description);
      if (!modalImg[0].dataset.description) {
        photoDescriptionStatic.hide();
      } else {
        photoDescriptionStatic.show();
      }
      photoDescription.val(modalImg[0].dataset.description);
    };
  });
}

function disableZoom() {
  const allImages = $("img[id]");
  allImages.each((index, img) => {
    img.onclick = null;
  });
}

function lazyLoad() {
  const allImages = $("img[id]");
  allImages.each((index, img) => {
    const svgForImg = $("#svg_" + img.id);
    const svgForImgElement = svgForImg[0];
    if (!svgForImgElement && img.src) return;
    if (svgForImgElement.getBoundingClientRect().top < window.innerHeight + window.pageYOffset) {
      img.src = img.dataset.lazySrc;
    }
  });
}

function prevPage() {
  const getParams = location.search
    .slice(1)
    .split('&')
    .filter(value => value)
    .map(p => p.split('='))
    .reduce((obj, [key, value]) => ({...obj, [key]: value}), {});
  window.location.href = window.location.origin + window.location.pathname + "?page="
    + (getParams.page > 1 ? getParams.page - 1 : 1) + "&size=" + getParams.size;
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
  window.location.href = window.location.origin + window.location.pathname + "?page="
    + (currentSize >= maxElements ? getParams.page : nextPage) + "&size=" + getParams.size;
}
