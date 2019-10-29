package io.global.photos.http.view;

import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

public class AlbumView {
	private final String id;
	private final Map<String, PhotoView> photoMap;
	private final String title;
	private final String description;

	private AlbumView(String id, String title, String description, Map<String, PhotoView> photoMap) {
		this.id = id;
		this.title = title;
		this.description = description;
		this.photoMap = photoMap;
	}

	public static AlbumView of(String id, String title, String description, Map<String, PhotoView> photoMap) {
		return new AlbumView(id, title, description, photoMap);
	}

	public String getId() {
		return id;
	}

	public String getTitle() {
		return title;
	}

	public String getDescription() {
		return description;
	}

	public Set<Map.Entry<String, PhotoView>> getPhotos() {
		return photoMap.entrySet();
	}

	@Nullable
	public Map.Entry<String, PhotoView> getAnyPhoto() {
		return photoMap.entrySet().stream().findAny().orElse(null);
	}
}
