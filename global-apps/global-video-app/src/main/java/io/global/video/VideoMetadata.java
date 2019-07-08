package io.global.video;

public final class VideoMetadata {
	private final String title, description, extension;

	public VideoMetadata(String title, String description, String extension) {
		this.title = title;
		this.description = description;
		this.extension = extension;
	}

	public String getTitle() {
		return title;
	}

	public String getDescription() {
		return description;
	}

	public String getExtension() {
		return extension;
	}
}
