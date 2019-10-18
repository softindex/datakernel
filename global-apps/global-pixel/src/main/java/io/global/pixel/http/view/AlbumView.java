package io.global.pixel.http.view;

import io.global.pixel.ot.Album;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.lang.Integer.MAX_VALUE;

public class AlbumView {
	private final String id;
	private final Map<String, PhotoView> photoMap;
	private String title;
	private String description;

	public AlbumView(String id, String title, String description, Map<String, PhotoView> photoMap) {
		this.id = id;
		this.title = title;
		this.description = description;
		this.photoMap = photoMap;
	}

	public static Map<String, AlbumView> from(Map<String, Album> albums) {
		HashMap<String, AlbumView> map = new HashMap<>();
		albums.forEach((key, album) -> {
			Map<String, PhotoView> photoMap = PhotoView.from(album.getPhotoMap());
			map.put(key, new AlbumView(key, album.getTitle(), album.getDescription(), photoMap));
		});
		return map;
	}

	public static AlbumView from(String id, Album album) {
		return from(id, album, 0, MAX_VALUE);
	}

	public static AlbumView from(String id, Album album, Integer page, Integer size) {
		long res = page.longValue() * size.longValue();
		long offset = ((int) res != res) ? MAX_VALUE : res;
		Map<String, PhotoView> photoMap = PhotoView.from(album.getPhotoMap(), offset, size);
		return new AlbumView(id, album.getTitle(), album.getDescription(), photoMap);
	}

	public static AlbumView of(String id, Album album) {
		return new AlbumView(id, album.getTitle(), album.getDescription(), PhotoView.from(album.getPhotoMap()));
	}

	public String getId() {
		return id;
	}

	public String getTitle() {
		return title;
	}

	public AlbumView setTitle(String title) {
		this.title = title;
		return this;
	}

	public String getDescription() {
		return description;
	}

	public AlbumView setDescription(String description) {
		this.description = description;
		return this;
	}

	public Set<Map.Entry<String, PhotoView>> getPhotos() {
		return photoMap.entrySet();
	}

	public Map.Entry<String, PhotoView> getAnyPhoto() {
		return photoMap.entrySet().stream().findAny().orElse(null);
	}
}
