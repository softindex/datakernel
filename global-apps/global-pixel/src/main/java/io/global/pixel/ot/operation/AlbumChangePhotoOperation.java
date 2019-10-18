package io.global.pixel.ot.operation;

import io.global.ot.value.ChangeValue;
import io.global.pixel.ot.Album;
import io.global.pixel.ot.Photo;

import java.util.Map;

public final class AlbumChangePhotoOperation implements AlbumOperation {
	private static final String EMPTY = "";
	private final String albumId;
	private final String photoId;
	private final ChangeValue<String> description;

	public AlbumChangePhotoOperation(String albumId, String photoId, ChangeValue<String> description) {
		this.albumId = albumId;
		this.photoId = photoId;
		this.description = description;
	}

	@Override
	public void apply(Map<String, Album> albumMap) {
		Album album = albumMap.get(albumId);
		Photo photo = album.getPhoto(photoId);
		photo.setDescription(description.getNext());
	}

	public AlbumChangePhotoOperation invert() {
		return new AlbumChangePhotoOperation(albumId, photoId, ChangeValue.of(description.getNext(), description.getPrev(), description.getTimestamp()));
	}

	@Override
	public boolean isEmpty() {
		return albumId.equals(EMPTY) &&
				getNextDescription().equals(EMPTY) && getPrevDescription().equals(EMPTY) &&
				photoId.equals(EMPTY);
	}

	public ChangeValue<String> getDescription() {
		return description;
	}

	public String getAlbumId() {
		return albumId;
	}

	public String getPhotoId() {
		return photoId;
	}

	public String getNextDescription() {
		return description.getNext();
	}

	public String getPrevDescription() {
		return description.getPrev();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof AlbumChangePhotoOperation)) return false;

		AlbumChangePhotoOperation that = (AlbumChangePhotoOperation) o;

		if (!getAlbumId().equals(that.getAlbumId())) return false;
		if (!getPhotoId().equals(that.getPhotoId())) return false;
		return getDescription().equals(that.getDescription());
	}

	@Override
	public int hashCode() {
		int result = getAlbumId().hashCode();
		result = 31 * result + getPhotoId().hashCode();
		result = 31 * result + getDescription().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "AlbumChangePhotoOperation{" +
				"albumId='" + albumId + '\'' +
				", photoId='" + photoId + '\'' +
				", description=" + description +
				'}';
	}
}
