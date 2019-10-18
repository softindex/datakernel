package io.global.pixel.ot.operation;

import io.global.pixel.ot.Album;

import java.util.Map;

public interface AlbumOperation {
	void apply(Map<String, Album> albumMap);

	boolean isEmpty();

	String getAlbumId();
}
