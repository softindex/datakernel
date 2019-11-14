package io.global.photos.ot;

import org.jetbrains.annotations.NotNull;

public final class Photo {
	public static final Photo EMPTY = Photo.create("", -1, "", 0, 0);
	private String description;
	private final long timeUpload;
	private final String filename;
	private final int width;
	private final int height;

	public Photo(@NotNull String description, long timeUpload, String filename, int width, int height) {
		this.description = description;
		this.timeUpload = timeUpload;
		this.filename = filename;
		this.width = width;
		this.height = height;
	}

	public static Photo create(@NotNull String description, long timeUpload, @NotNull String filename, int width, int heigth) {
		return new Photo(description, timeUpload, filename, width, heigth);
	}

	public String getDescription() {
		return description;
	}

	public long getTimeUpload() {
		return timeUpload;
	}

	public String getFilename() {
		return filename;
	}

	public int getHeight() {
		return height;
	}

	public int getWidht() {
		return width;
	}

	public Photo setDescription(String description) {
		this.description = description;
		return this;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Photo)) return false;

		Photo photo = (Photo) o;

		if (getTimeUpload() != photo.getTimeUpload()) return false;
		if (!getDescription().equals(photo.getDescription())) return false;
		return getFilename().equals(photo.getFilename());
	}

	@Override
	public int hashCode() {
		int result = getDescription().hashCode();
		result = 31 * result + (int) (getTimeUpload() ^ (getTimeUpload() >>> 32));
		result = 31 * result + getFilename().hashCode();
		return result;
	}
}
