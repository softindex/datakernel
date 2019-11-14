package io.global.photos.ot.operation;

import io.global.photos.ot.Album;

import java.util.Map;

public interface AlbumOperation {
	void apply(Map<String, Album> albumMap);

	boolean isEmpty();

	String getAlbumId();
}
