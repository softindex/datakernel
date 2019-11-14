const allImages = $("img[id]");

$(document).ready(() => {
// === Preload image ====
  allImages.each((_, img) => {
    const $img = $(img);

    $img.one("load", () => {
      $img.show();
      $("#svg_" + img.id).remove();
    });
  });
});

window.onload = () => {
  const modal = $("#modal");
  const modalImg = $("#modalImg");
  const close = $("#close");
  const downloadButton = $("#downloadButton");

  const imageCheckboxs = $("[id^=image_checkbox_]");

  const photoDescription = $("#photo_description");
  const editPhotoDescription = $("#editPhotoDescription");
  const editDescriptionButtonGroup = $("#editDescriptionButtonGroup");

  const uploadImageButton = $("#uploadImageButton");
  const createAlbumButton = $("#create-album");
  
  function lazyLoad() {
    allImages.each((index, img) => {
      const svgForImg = $("#svg_" + img.id);
      const svgForImgElement = svgForImg[0];
      if (!svgForImgElement  && img.src) return;
      if (svgForImgElement.getBoundingClientRect().top < window.innerHeight + window.pageYOffset) {
        img.src = img.dataset.lazySrc;
      }
    });
  }

  lazyLoad();
  window.addEventListener("scroll", lazyLoad);
  window.addEventListener("resize", lazyLoad);

  function enableZoom() {
    allImages.each((index, img) => {
      img.onclick = () => {
        const $img = $(img);
        downloadButton.attr("href", $img[0].dataset.fullImg);
        modal.css({display: "block"});
        modalImg.attr("src", $img[0].dataset.thumbnailImg);
        modalImg.attr("data-description", $img[0].dataset.description);
        modalImg.attr("data-url", $img[0].dataset.url);
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

  // ======== creation album handling ======
  const cancelCreationAlbum = $("#cancel_creation_album");
  const createAlbum = $("#create_album");
  const albumForm = $("#new_album_form");
  const titleAlbum = $("#title_album");
  const descriptionAlbum = $("#description_album");

  createAlbum.click(() => {
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

  cancelCreationAlbum.click(() => {
    albumForm.toggle();
    uploadImageButton.removeClass('text-secondary');
    createAlbumButton.removeClass('text-secondary');
    disableSelection();
  });

  createAlbumButton.click(() => {
    disableZoom();
    $(".tools").addClass('text-secondary');
    uploadImageButton.addClass('text-secondary');
    createAlbumButton.addClass('text-secondary');
    albumBar.css({display: "none"});
    albumForm.toggle();
    enableSelection();
  });
  // ======== end creation album handling ======

  // ======== deletion album handling ======
  const cancelDeletionPhotos = $("#cancelDeletionPhotos");
  const deletePhotosForm = $("#delete_photos_form");
  const deletePhotos = $("#deletePhotos");

  deletePhotos.click(() => {
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

  cancelDeletionPhotos.click(() => {
    albumBar.css({display: ""});
    toolsButton.attr("disabled", false);
    createAlbumButton.removeClass("disabled");
    uploadImageButton.removeClass("disabled");
    disableSelection();
    deletePhotosForm.toggle();
    enableZoom();
  });

  $("#delete_photos_tool").click(() => {
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
  // ======== end deletion album handling ======

  // ======== edit photo description handling ======
  $("#savePhotoDescription").click(() => {
    fetch(modalImg[0].dataset.url, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      },
      body: JSON.stringify([photoDescription.val()]),
      method: 'POST',
    }).then(() => location.reload(), console.error);
  });

  $("#cancelEditingPhotoDescription").click(() => {
    downloadButton.show();
    editDescriptionButtonGroup.css({display: "none"});
    editPhotoDescription.css({display: ""});
    photoDescription.val(modalImg[0].dataset.description);
    photoDescription.prop('readonly', true);
  });

  editPhotoDescription.click(() => {
    downloadButton.hide();
    editDescriptionButtonGroup.css({display: ""});
    editPhotoDescription.css({display: "none"});
    photoDescription.prop('readonly', false).focus();
  });

  close.click(() => {
    photoDescription.val("");
    editDescriptionButtonGroup.css({display: "none"});
    editPhotoDescription.css({display: ""});
    albumBar.css({display: ""});
    modal.css({display: "none"});
  });
  // ======== end edit photo description handling ======

  // ======== move album handling ======
  const movePhotosForm = $("#move_photos_form");
  const movePhotosTool = $("#move_photos_tool");
  const movePhotos = $("#movePhotos");
  const cancelMovingPhotos = $("#cancelMovingPhotos");
  const albumSelect = $("#albumSelect");

  movePhotos.click(() => {
    const selectedImages = imageCheckboxs.filter('.image-checkbox-checked').map((index, value) => value.dataset.id).get();
    fetch(movePhotosForm[0].dataset.url, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      },
      body: JSON.stringify([albumSelect.val(), selectedImages]),
      method: 'POST',
    }).then(() => location.reload(), console.error);
  });

  cancelMovingPhotos.click(() => {
    movePhotosForm.toggle();
    uploadImageButton.removeClass('disabled');
    createAlbumButton.removeClass('disabled');
    toolsButton.attr("disabled", false);
    enableZoom();
    disableSelection();
  });

  movePhotosTool.click(() => {
    movePhotosForm.toggle();
    tools.collapse("hide");
    toolsButton.attr("disabled", true);
    uploadImageButton.addClass('disabled');
    createAlbumButton.addClass('disabled');
    disableZoom();
    enableSelection();
  });
  // ======== end move album handling ======

  // ======== update album handling ======
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
  // ======== end update album handling ======

  // ======== pagination handling ======
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
    if (maxElements === 0) {
      pagination.toggle("hide");
      return;
    }

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
  // ======== end pagination handling ======


  const uploadImages = $("#upload_images");
  // Upload images
  uploadImages.click(() => {
    let progressBar = $('#progress');
    const newAttachments = $('[id$="_attachment"]').get();
    const formData = new FormData();
    $("#attachments").hide();
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
};

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
