package io.global.video.pojo;

import static io.datakernel.util.StringFormatUtils.limit;

public final class VideoMetadata {
	private final String title, description;

	public VideoMetadata(String title, String description) {
		this.title = title;
		this.description = description;
	}

	public String getTitle() {
		return title;
	}

	public String getDescription() {
		return description;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		VideoMetadata that = (VideoMetadata) o;

		if (!title.equals(that.title)) return false;
		if (!description.equals(that.description)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = title.hashCode();
		result = 31 * result + description.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "VideoMetadata{" +
				"title='" + title + '\'' +
				", description='" + limit(description, 30) + '\'' +
				'}';
	}
}
