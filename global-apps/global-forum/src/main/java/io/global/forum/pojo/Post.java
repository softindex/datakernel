package io.global.forum.pojo;

import static io.datakernel.util.StringFormatUtils.limit;

public final class Post {
	private final UserId author;
	private final String content;
	private final long timestamp;

	public Post(UserId author, String content, long timestamp) {
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

		Post post = (Post) o;

		if (timestamp != post.timestamp) return false;
		if (!author.equals(post.author)) return false;
		if (!content.equals(post.content)) return false;

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
		return "Post{" +
				"author=" + author +
				", content='" + limit(content, 30) + '\'' +
				", timestamp=" + timestamp +
				'}';
	}
}
