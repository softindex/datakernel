package io.global.pixel.ot.operation;

import io.global.pixel.ot.Album;

import java.util.Map;
import java.util.Objects;

public final class AlbumAddOperation implements AlbumOperation {
	public static final AlbumAddOperation EMPTY = new AlbumAddOperation("", "", "",false);
	private final String title;
	private final String albumId;
	private final boolean remove;
	private final String description;

	public AlbumAddOperation(String albumId, String title, String description, boolean remove) {
		this.description = description;
		this.albumId = albumId;
		this.remove = remove;
		this.title = title;
	}

	@Override
	public void apply(Map<String, Album> albumMap) {
		if (remove) {
			albumMap.remove(albumId);
		} else {
			albumMap.put(albumId, Album.create(title, description));
		}
	}

	@Override
	public boolean isEmpty() {
		return title.equals(EMPTY.title) &&
				albumId.equals(EMPTY.albumId) &&
				description.equals(EMPTY.description);
	}

	public AlbumAddOperation invert() {
		return new AlbumAddOperation(albumId, title, description, !remove);
	}

	public boolean isInversion(AlbumAddOperation other) {
		return Objects.equals(title, other.title) &&
				albumId.equals(other.albumId) &&
				remove != other.remove;
	}

	public String getTitle() {
		return title;
	}

	public String getAlbumId() {
		return albumId;
	}

	public boolean isRemove() {
		return remove;
	}

	public String getDescription() {
		return description;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof AlbumAddOperation)) return false;

		AlbumAddOperation that = (AlbumAddOperation) o;

		if (isRemove() != that.isRemove()) return false;
		if (!getTitle().equals(that.getTitle())) return false;
		if (!getAlbumId().equals(that.getAlbumId())) return false;
		return getDescription().equals(that.getDescription());
	}

	@Override
	public int hashCode() {
		int result = getTitle().hashCode();
		result = 31 * result + getAlbumId().hashCode();
		result = 31 * result + (isRemove() ? 1 : 0);
		result = 31 * result + getDescription().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "AlbumAddOperation{" +
				"title='" + title + '\'' +
				", albumId='" + albumId + '\'' +
				", remove=" + remove +
				", description='" + description + '\'' +
				'}';
	}
}
