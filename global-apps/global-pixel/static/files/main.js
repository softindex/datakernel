window.onload = () => {
  const modal = $("#modal");
  const modalImg = $("#modalImg");
  const close = $("#close");
  const allImages = $("img").filter((index, img) => img.id.length > 0);

  const imageCheckboxs = $("[id^=image_checkbox_]");

  const photoDescription = $("#photo_description");
  const editPhotoDescription = $("#editPhotoDescription");
  const editDescriptionButtonGroup = $("#editDescriptionButtonGroup");

  const uploadImageButton = $("#uploadImageButton");
  const createAlbumButton = $("#create-album");

  function enableZoom() {
    allImages.each((index, img) => {
      img.onclick = function (e) {
        modal.css({display: "block"});
        modalImg.attr("src", this.dataset.fullImg);
        modalImg.attr("data-description", this.dataset.description);
        modalImg.attr("data-url", this.dataset.url);
        photoDescription.val(modalImg[0].dataset.description);
      };
    });
  }

  function disableZoom() {
    allImages.each((index, img) => {
      img.onclick = null;
    });
  }

  function enableSelection() {
    imageCheckboxs.on("click", function (e) {
      $(this).toggleClass('image-checkbox-checked');
      var $checkbox = $(this).find('input[type="checkbox"]');
      $checkbox.prop("checked", !$checkbox.prop("checked"));
      e.preventDefault();
    });
  }

  function disableSelection() {
    imageCheckboxs.off();
    imageCheckboxs.each(function () {
      $(this).removeClass('image-checkbox-checked');
    });
  }

  enableZoom();
  const gallery = $("#gallery");
  const albumBar = $("#album_bar");
  const toolsButton = $("#tools_button");
  const tools = $("#tools");

  $("#image_attachment").change(function () {
    if (this.files) {
      Array.from(this.files).forEach(file => {
        const reader = new FileReader();
        reader.onload = function (e) {
          gallery.append($("<div class=\"mb-3 pics animation all 2\"><img class=\"img-fluid\" src=\"" + e.target.result + "\"></div>"))
        };
        reader.readAsDataURL(file);
      });
    }
  });

  const cancelCreationAlbum = $("#cancel_creation_album");
  const createAlbum = $("#create_album");
  const albumForm = $("#new_album_form");
  const titleAlbum = $("#title_album");
  const descriptionAlbum = $("#description_album");

  createAlbum.click((e) => {
    const selectedImages = imageCheckboxs.filter('.image-checkbox-checked').map((index, value) => value.dataset.id).get();
    fetch(albumForm[0].dataset.url, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      },
      method: 'POST',
      body: JSON.stringify([
        titleAlbum[0].value,
        descriptionAlbum[0].value,
        selectedImages
      ])
    }).then((response) => {
      if (response.redirected) {
        window.location.href = response.url;
      } else {
        location.reload()
      }
    }, console.error);
  });

  cancelCreationAlbum.click((e) => {
    albumForm.toggle();
    uploadImageButton.removeClass('disabled');
    createAlbumButton.removeClass('disabled');
    disableSelection();
  });

  createAlbumButton.click((e) => {
    disableZoom();
    uploadImageButton.addClass('disabled');
    createAlbumButton.addClass('disabled');
    albumBar.css({display: "none"});
    albumForm.toggle();
    enableSelection();
  });

  const cancelDeletionPhotos = $("#cancelDeletionPhotos");
  const deletePhotosForm = $("#delete_photos_form");
  const deletePhotos = $("#deletePhotos");

  deletePhotos.click((e) => {
    const selectedImages = imageCheckboxs.filter('.image-checkbox-checked').map((index, value) => value.dataset.id).get();
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

  cancelDeletionPhotos.click((e) => {
    albumBar.css({display: ""});
    toolsButton.attr("disabled", false);
    createAlbumButton.removeClass("disabled");
    uploadImageButton.removeClass("disabled");
    disableSelection();
    deletePhotosForm.toggle();
    enableZoom();
  });

  $("#delete_photos_tool").click((e) => {
    albumBar.css({display: "none"});
    createAlbumButton.addClass("disabled");
    uploadImageButton.addClass("disabled");
    tools.collapse("hide");
    toolsButton.attr("disabled", true);
    deletePhotosForm.toggle();
    disableZoom();
    enableSelection();
  });

  const deleteAlbum = $("#delete_album_tool");
  deleteAlbum.click((e) => {
    if (confirm("Are you sure you want to delete this?")) {
      fetch(deleteAlbum[0].dataset.url, {
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

  $("#savePhotoDescription").click((e) => {
    fetch(modalImg[0].dataset.url, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      },
      body: JSON.stringify([photoDescription.val()]),
      method: 'POST',
    }).then(() => location.reload(), console.error);
  });

  $("#cancelEditingPhotoDescription").click((e) => {
    editDescriptionButtonGroup.css({display: "none"});
    editPhotoDescription.css({display: ""});
    photoDescription.val(modalImg[0].dataset.description);
    photoDescription.prop('readonly', true);
  });

  editPhotoDescription.click((e) => {
    editDescriptionButtonGroup.css({display: ""});
    editPhotoDescription.css({display: "none"});
    photoDescription.prop('readonly', false).focus();
  });

  close.click((e) => {
    photoDescription.val("");
    editDescriptionButtonGroup.css({display: "none"});
    editPhotoDescription.css({display: ""});
    albumBar.css({display: ""});
    modal.css({display: "none"});
  });

  const movePhotosForm = $("#move_photos_form");
  const movePhotosTool = $("#move_photos_tool");
  const movePhotos = $("#movePhotos");
  const cancelMovingPhotos = $("#cancelMovingPhotos");
  const albumSelect = $("#albumSelect");

  movePhotos.click((e) => {
    const selectedImages = imageCheckboxs.filter('.image-checkbox-checked').map((index, value) => value.dataset.id).get();
    fetch(movePhotosForm[0].dataset.url, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      },
      body: JSON.stringify([albumSelect.val(), selectedImages]),
      method: 'POST',
    }).then(() => location.reload(), console.error);
  });

  cancelMovingPhotos.click((e) => {
    movePhotosForm.toggle();
    uploadImageButton.removeClass('disabled');
    createAlbumButton.removeClass('disabled');
    toolsButton.attr("disabled", false);
    enableZoom();
    disableSelection();
  });

  movePhotosTool.click((e) => {
    movePhotosForm.toggle();
    tools.collapse("hide");
    toolsButton.attr("disabled", true);
    uploadImageButton.addClass('disabled');
    createAlbumButton.addClass('disabled');
    disableZoom();
    enableSelection();
  });

  const updateAlbumForm = $("#update_album_form");
  const updateAlbumTool = $("#update_album_tool");
  const updateAlbum = $("#update_album");
  const cancelUpdatingAlbum = $("#cancel_updating_album");
  const updateDesciptionAlbum = $("#update_description_album");
  const updateTitleAlbum = $("#update_title_album");
  const currentDescriptionAlbum = $("#current_description_album");

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

  updateAlbum.click(() => {
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
  });
};
