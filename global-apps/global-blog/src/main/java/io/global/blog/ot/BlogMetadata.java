package io.global.blog.ot;

public final class BlogMetadata {
	public static final BlogMetadata EMPTY = new BlogMetadata("", "");

	private final String name;
	private final String description;

	public BlogMetadata(String name, String description) {
		this.name = name;
		this.description = description;
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	@Override
	public String toString() {
		return "BlogMetadata{name='" + name + "\', description='" + description + "'}";
	}
}
