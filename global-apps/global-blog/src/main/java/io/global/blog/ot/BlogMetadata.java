package io.global.blog.ot;

public final class BlogMetadata {
	public static final BlogMetadata EMPTY = new BlogMetadata("", "");

	private final String title;
	private final String description;

	public BlogMetadata(String title, String description) {
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
	public String toString() {
		return "BlogMetadata{title='" + title + "\', description='" + description + "'}";
	}
}
