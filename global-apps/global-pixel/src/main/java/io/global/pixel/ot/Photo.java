package io.global.pixel.ot;

import org.jetbrains.annotations.NotNull;

public final class Photo {
	public static final Photo EMPTY = Photo.create("", -1, "");
	private String description;
	private final long timeUpload;
	private final String filename;

	private Photo(@NotNull String description, long timeUpload, @NotNull String filename) {
		this.description = description;
		this.timeUpload = timeUpload;
		this.filename = filename;
	}

	public static Photo create(@NotNull String description, long timeUpload, @NotNull String filename) {
		return new Photo(description, timeUpload, filename);
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
