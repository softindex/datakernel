package io.global.comm.ot;

import org.jetbrains.annotations.Nullable;

public final class AppMetadata {
	public static final AppMetadata EMPTY = new AppMetadata("", "");

	@Nullable
	private final String title;

	@Nullable
	private final String description;

	public AppMetadata(@Nullable String title, @Nullable String description) {
		this.title = title;
		this.description = description;
	}

	@Nullable
	public String getTitle() {
		return title;
	}

	@Nullable
	public String getDescription() {
		return description;
	}

	@Override
	public String toString() {
		return "AppMetadata{title='" + title + "', description='" + description + "'}";
	}
}
