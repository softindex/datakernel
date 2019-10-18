package io.global.pixel.ot;

import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Album {
	private static final String EMPTY = "";
	private final Map<String, Photo> photoMap;
	private String title;
	private String description;

	private Album(String title, String description, Map<String, Photo> photoMap) {
		this.title = title;
		this.description = description;
		this.photoMap = photoMap;
	}

	public static Album create(String title) {
		return new Album(title, EMPTY, new LinkedHashMap<>());
	}

	public static Album create(String title, String description) {
		return new Album(title, description, new LinkedHashMap<>());
	}

	public String getTitle() {
		return title;
	}

	public Album setTitle(String title) {
		this.title = title;
		return this;
	}

	public String getDescription() {
		return description;
	}

	public Album setDescription(String description) {
		this.description = description;
		return this;
	}

	public Map<String, Photo> getPhotoMap() {
		return photoMap;
	}

	public Album addPhoto(String id, Photo photo) {
		photoMap.put(id, photo);
		return this;
	}

	public Photo removePhoto(String photoId) {
		return photoMap.remove(photoId);
	}

	@Nullable
	public Photo getPhoto(String id) {
		return photoMap.get(id);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Album)) return false;

		Album album = (Album) o;

		if (!getTitle().equals(album.getTitle())) return false;
		if (!getDescription().equals(album.getDescription())) return false;
		return getPhotoMap().equals(album.getPhotoMap());
	}

	@Override
	public int hashCode() {
		int result = 0;
		result = 31 * result + getTitle().hashCode();
		result = 31 * result + getDescription().hashCode();
		result = 31 * result + getPhotoMap().hashCode();
		return result;
	}
}
