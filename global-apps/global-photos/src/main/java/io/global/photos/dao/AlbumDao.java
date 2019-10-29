package io.global.photos.dao;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.photos.ot.Album;
import io.global.photos.ot.Photo;

import java.util.Map;
import java.util.Set;

public interface AlbumDao {
	StacklessException PHOTO_NOT_FOUND_EXCEPTION = new StacklessException("Cannot find photo for the given id");
	StacklessException ALBUM_NOT_FOUND_EXCEPTION = new StacklessException("Album is not found with given id");
	StacklessException ALBUM_ALREADY_EXIST = new StacklessException("Album is already exist with given id");
	String ROOT_ALBUM = "all";
	String FULL_SIZED_THUMBNAIL = "full";

	Promise<Album> getAlbum();

	Promise<String> generatePhotoId();

	Promise<ChannelSupplier<ByteBuf>> loadPhoto(String thumbnail, String photoId, long offset, long limit);

	default Promise<ChannelSupplier<ByteBuf>> loadPhoto(String photoId, long offset, long limit) {
		return loadPhoto(FULL_SIZED_THUMBNAIL, photoId, offset, limit);
	}

	default Promise<ChannelSupplier<ByteBuf>> loadPhoto(String photoId) {
		return loadPhoto(photoId, 0, -1);
	}

	default Promise<ChannelSupplier<ByteBuf>> loadPhoto(String thumbnail, String photoId) {
		return loadPhoto(thumbnail, photoId, 0, -1);
	}

	Promise<ChannelConsumer<ByteBuf>> addPhoto(String id, String filename, String description);

	Promise<Photo> getPhoto(String id);

	Promise<Map<String, Photo>> getPhotos();

	Promise<Void> removePhoto(String id);

	Promise<Void> removePhotos(Set<String> photoIds);

	Promise<Void> removeAllPhotos();

	Promise<Boolean> photoExists(String id);

	Promise<Long> photoSize(String thumbnail, String id);

	default Promise<Long> photoSize(String id) {
		return photoSize(FULL_SIZED_THUMBNAIL, id);
	}

	Promise<Void> updatePhoto(String photoId, String description);
}
