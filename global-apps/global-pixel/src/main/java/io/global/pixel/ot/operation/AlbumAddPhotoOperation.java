package io.global.pixel.ot.operation;

import io.global.pixel.ot.Album;
import io.global.pixel.ot.Photo;

import java.util.Map;
import java.util.Objects;

public final class AlbumAddPhotoOperation implements AlbumOperation {
	public static final AlbumOperation EMPTY = new AlbumAddPhotoOperation("", "", Photo.EMPTY, false);
	private final String albumId;
	private final String photoId;
	private final Photo photo;
	private final boolean remove;

	public AlbumAddPhotoOperation(String albumId, String photoId, Photo photo, boolean remove) {
		this.albumId = albumId;
		this.photoId = photoId;
		this.photo = photo;
		this.remove = remove;
	}

	@Override
	public void apply(Map<String, Album> albumMap) {
		Album album = albumMap.get(albumId);
		if (remove) {
			album.removePhoto(photoId);
		} else {
			album.addPhoto(photoId, photo);
		}
	}

	@Override
	public boolean isEmpty() {
		return albumId.equals("") &&
				photoId.equals("") &&
				photo.equals(Photo.EMPTY);
	}

	public AlbumAddPhotoOperation invert() {
		return new AlbumAddPhotoOperation(albumId, photoId, photo, !remove);
	}

	public boolean isInversion(AlbumAddPhotoOperation other) {
		return Objects.equals(albumId, other.albumId) &&
				photoId.equals(other.photoId) &&
				photo.equals(other.photo) &&
				remove != other.remove;
	}

	public String getAlbumId() {
		return albumId;
	}

	public String getPhotoId() {
		return photoId;
	}

	public Photo getPhoto() {
		return photo;
	}

	public boolean isRemove() {
		return remove;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof AlbumAddPhotoOperation)) return false;

		AlbumAddPhotoOperation that = (AlbumAddPhotoOperation) o;

		if (isRemove() != that.isRemove()) return false;
		if (!getAlbumId().equals(that.getAlbumId())) return false;
		if (!getPhotoId().equals(that.getPhotoId())) return false;
		return getPhoto().equals(that.getPhoto());
	}

	@Override
	public int hashCode() {
		int result = getAlbumId().hashCode();
		result = 31 * result + getPhotoId().hashCode();
		result = 31 * result + getPhoto().hashCode();
		result = 31 * result + (isRemove() ? 1 : 0);
		return result;
	}

	@Override
	public String toString() {
		return "AlbumAddPhotoOperation{" +
				"albumId='" + albumId + '\'' +
				", photoId='" + photoId + '\'' +
				", photo=" + photo +
				", remove=" + remove +
				'}';
	}
}
