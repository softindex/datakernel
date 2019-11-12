package io.global.forum.ot;

public final class ForumMetadata {
	public static final ForumMetadata EMPTY = new ForumMetadata("", "");

	private final String title;
	private final String description;

	public ForumMetadata(String title, String description) {
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
		return "ForumMetadata{title='" + title + "\', description='" + description + "'}";
	}
}
