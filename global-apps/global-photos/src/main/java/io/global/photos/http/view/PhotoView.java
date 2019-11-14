package io.global.photos.http.view;

import io.global.photos.ot.Photo;
import org.jetbrains.annotations.Nullable;

public final class PhotoView {
	private final long timeUpload;
	private final String filename;
	private final int width;
	private final int height;
	private final float ratio;
	private final String description;
	@Nullable
	private final String base64;

	public PhotoView(String description, long timeUpload, String filename, int width, int height, float ratio, @Nullable String base64) {
		this.description = description;
		this.timeUpload = timeUpload;
		this.filename = filename;
		this.width = width;
		this.height = height;
		this.ratio = ratio;
		this.base64 = base64;
	}

	public static PhotoView of(Photo value, @Nullable String base64) {
		int w = value.getWidht();
		int h = value.getHeight();
		return new PhotoView(value.getDescription(), value.getTimeUpload(), value.getFilename(), w, h, 100.0f * h / w, base64);
	}

	public String getPhotoDescription() {
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

	public int getWidth() {
		return width;
	}

	public float getRatio() {
		return ratio;
	}

	@Nullable
	public String base64() {
		return base64;
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
