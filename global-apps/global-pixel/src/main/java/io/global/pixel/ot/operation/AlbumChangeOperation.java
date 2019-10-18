package io.global.pixel.ot.operation;

import io.datakernel.util.Tuple2;
import io.global.ot.value.ChangeValue;
import io.global.pixel.ot.Album;

import java.util.Map;

public final class AlbumChangeOperation implements AlbumOperation {
	private static final String EMPTY = "";
	private final String albumId;
	private final ChangeValue<Tuple2<String, String>> metadata;

	public AlbumChangeOperation(String albumId, Album prev, Album next) {
		this.albumId = albumId;
		long now = System.currentTimeMillis();
		this.metadata = ChangeValue.of(new Tuple2<>(prev.getTitle(), prev.getDescription()), new Tuple2<>(next.getTitle(), next.getDescription()), now);
	}

	public AlbumChangeOperation(String albumId, ChangeValue<Tuple2<String, String>> metadata) {
		this.albumId = albumId;
		this.metadata = metadata;
	}

	public String getAlbumId() {
		return albumId;
	}

	public String getPrevTitle() {
		return metadata.getPrev().getValue1();
	}

	public String getNextTitle() {
		return metadata.getNext().getValue1();
	}

	public ChangeValue<Tuple2<String, String>> getMetadata() {
		return metadata;
	}

	public String getPrevDescription() {
		return metadata.getPrev().getValue2();
	}

	public String getNextDescription() {
		return metadata.getNext().getValue2();
	}

	public AlbumChangeOperation invert() {
		return new AlbumChangeOperation(
				albumId,
				ChangeValue.of(metadata.getNext(), metadata.getPrev(), metadata.getTimestamp()));
	}

	@Override
	public void apply(Map<String, Album> albumMap) {
		Album album = albumMap.get(albumId);
		album.setTitle(metadata.getNext().getValue1());
		album.setDescription(metadata.getNext().getValue2());
	}

	@Override
	public boolean isEmpty() {
		return albumId.equals(EMPTY) &&
				getNextTitle().equals(EMPTY) && getPrevTitle().equals(EMPTY) &&
				getNextDescription().equals(EMPTY) && getPrevDescription().equals(EMPTY);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof AlbumChangeOperation)) return false;

		AlbumChangeOperation that = (AlbumChangeOperation) o;

		if (!getAlbumId().equals(that.getAlbumId())) return false;
		return getMetadata().equals(that.getMetadata());
	}

	@Override
	public int hashCode() {
		int result = getAlbumId().hashCode();
		result = 31 * result + getMetadata().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "AlbumChangeOperation{" +
				"albumId='" + albumId + '\'' +
				", metadata=" + metadata +
				'}';
	}
}
