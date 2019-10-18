package io.global.pixel.http.view;

import io.global.pixel.ot.Photo;

import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.Integer.MAX_VALUE;

public final class PhotoView {
	private String description;
	private final long timeUpload;
	private final String filename;

	private PhotoView(String description, long timeUpload, String filename) {
		this.description = description;
		this.timeUpload = timeUpload;
		this.filename = filename;
	}

	public static PhotoView of(Photo value) {
		return new PhotoView(value.getDescription(), value.getTimeUpload(), value.getFilename());
	}

	public static Map<String, PhotoView> from(Map<String, Photo> photoMap) {
		return from(photoMap, 0, MAX_VALUE);
	}

	public static Map<String, PhotoView> from(Map<String, Photo> photoMap, long offset, Integer size) {
		return photoMap.entrySet()
				.stream()
				.skip(offset)
				.limit(size)
				.collect(Collectors.toMap(Map.Entry::getKey, e -> PhotoView.of(e.getValue())));
	}

	public String getPhotoDescription() {
		return description;
	}

	public PhotoView setDescription(String description) {
		this.description = description;
		return this;
	}

	public long getTimeUpload() {
		return timeUpload;
	}

	public String getFilename() {
		return filename;
	}

	@Override
	public String toString() {
		return "PhotoView{" +
				"description='" + description + '\'' +
				", timeUpload=" + timeUpload +
				", filename='" + filename + '\'' +
				'}';
	}
}
