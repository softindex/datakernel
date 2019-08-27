package io.global.forum.ot;

public final class ForumMetadata {
	private final String name;
	private final String description;

	public ForumMetadata(String name, String description) {
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
		return "ForumMetadata{name='" + name + "\', description='" + description + "'}";
	}
}
