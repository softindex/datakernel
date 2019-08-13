package io.global.video.pojo;

import static io.datakernel.util.StringFormatUtils.limit;

public final class Comment {
	private final UserId author;
	private final String content;
	private final long timestamp;

	public Comment(UserId author, String content, long timestamp) {
		this.author = author;
		this.content = content;
		this.timestamp = timestamp;
	}

	public UserId getAuthor() {
		return author;
	}

	public String getContent() {
		return content;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Comment comment = (Comment) o;

		if (timestamp != comment.timestamp) return false;
		if (!author.equals(comment.author)) return false;
		if (!content.equals(comment.content)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = author.hashCode();
		result = 31 * result + content.hashCode();
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "Comment{" +
				"author=" + author +
				", content='" + limit(content, 30) + '\'' +
				", timestamp=" + timestamp +
				'}';
	}
}
